"""Acceso autorizado a adjuntos y precondición de almacenamiento para CHAT1."""
import base64
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import struct
import sys
from threading import BoundedSemaphore, Lock
import time

from chat_model import ATTACH, SECURE_ATTACH, HASH, ID, Rejected, require, valid_filename
from media_store import (BLOCK, MAX_FILE, BlobStore, GET, PUT, SEAL, STATUS,
                         directory_lock, key, remote, sync_dir)

OPERATIONS = {
    "upload-encrypted-init": {"conversation", "size", "sha256", "message", "device", "epoch", "envelope"},
    "upload-init": {"conversation", "filename", "size", "sha256", "content_type"},
    "upload-chunk": {"transfer", "offset", "block"},
    "upload-status": {"transfer"},
    "upload-complete": {"transfer"},
    "upload-cancel": {"transfer"},
    "download-init": {"attachment"},
    "download-chunk": {"attachment", "offset"},
    "attachment-status": {"attachment"},
}
TERMINAL = {"COMMITTED", "ABORTED", "REJECTED", "CANCELLED"}
TYPES = {"image/png", "image/jpeg", "application/pdf", "application/zip", "application/octet-stream"}


def result_status(result):
    if result["consensus"] == "COMMITTED":
        app = result.get("application", {})
        if app.get("status") == "APPLIED":
            return "COMMITTED"
        if app.get("status") == "REJECTED":
            return "REJECTED"
        return "PENDING"
    return {"ABORTED": "ABORTED", "CONFLICT": "REJECTED"}.get(result["consensus"], "PENDING")


def preview_dimensions(width, height):
    return 0 < width <= 8192 and 0 < height <= 8192 and width * height <= 8_000_000


def jpeg_dimensions(stream):
    stream.seek(2)
    while stream.tell() < 256 * 1024:
        marker = stream.read(2)
        if len(marker) != 2 or marker[0] != 255 or marker[1] in (0, 0xD9, 0xDA):
            return None
        if marker[1] == 255:
            stream.seek(-1, 1)
            continue
        field = stream.read(2)
        if len(field) != 2:
            return None
        length = int.from_bytes(field, "big")
        if length < 2:
            return None
        if marker[1] in {0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF}:
            size = stream.read(5)
            if length >= 8 and len(size) == 5:
                return int.from_bytes(size[3:], "big"), int.from_bytes(size[1:3], "big")
            return None
        stream.seek(length - 2, 1)
    return None


def detected_type(path):
    with path.open("rb") as stream:
        head = stream.read(24)
        if len(head) == 24 and head.startswith(b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR"):
            if preview_dimensions(*struct.unpack(">II", head[16:24])):
                return "image/png"
        if head.startswith(b"\xff\xd8\xff"):
            size = jpeg_dimensions(stream)
            if size and preview_dimensions(*size):
                return "image/jpeg"
        if head.startswith(b"%PDF-"):
            return "application/pdf"
        if head.startswith((b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")):
            return "application/zip"
    return "application/octet-stream"


class Media:
    @classmethod
    @contextmanager
    def open(cls, config, directory, consensus, gateway_key):
        if config is None:
            yield None
            return
        cfg = json.loads(Path(config).read_text(encoding="utf-8"))
        require(cfg.get("algorithm") == ("raft" if consensus.coordinator is None else "2pc"),
                "INVALID_MEDIA_ALGORITHM")
        nodes = cfg.get("nodes", {})
        require(set(nodes) == set(consensus.addresses), "INVALID_MEDIA_MEMBERS")
        endpoints = {}
        for name, target in nodes.items():
            require(isinstance(target, list) and len(target) == 2 and isinstance(target[0], str)
                    and type(target[1]) is int and 1 <= target[1] < 65536, "INVALID_MEDIA_ENDPOINT")
            endpoints[name] = tuple(target)
        require(len(set(endpoints.values())) == len(endpoints) == 3, "INVALID_MEDIA_ENDPOINT")
        secret = key()
        require(secret not in (consensus.secret, gateway_key), "MEDIA_KEY_MUST_BE_SEPARATE")
        directory.mkdir(parents=True, exist_ok=True)
        with directory_lock(directory), ThreadPoolExecutor(max_workers=4) as workers:
            identity_path = directory / "algorithm"
            if identity_path.exists():
                require(identity_path.read_text(encoding="ascii") == cfg["algorithm"],
                        "MEDIA_REQUIRES_SEPARATE_DATA")
            else:
                with identity_path.open("x", encoding="ascii") as stream:
                    stream.write(cfg["algorithm"])
                    stream.flush()
                    os.fsync(stream.fileno())
                sync_dir(directory)
            yield cls(directory, endpoints, secret, workers, cfg["algorithm"])

    def __init__(self, directory, nodes, secret, workers, algorithm):
        self.root = Path(directory)
        self.uploads = self.root / "uploads"
        self.uploads.mkdir(parents=True, exist_ok=True)
        self.cache = BlobStore(self.root / "downloads")
        self.nodes, self.secret, self.workers = nodes, secret, workers
        self.quorum = 2 if algorithm == "raft" else 3
        self.locks = [Lock() for _ in range(64)]
        self.active = set()
        self.jobs = Lock()
        self.slots = BoundedSemaphore(4)
        self.catalog = Lock()
        self.verified = OrderedDict()
        self.verification = Lock()

    def lock(self, ident):
        return self.locks[int(ident[:2], 16) % len(self.locks)]

    def start(self, ident, task):
        with self.jobs:
            if ident in self.active or not self.slots.acquire(blocking=False):
                return False
            self.active.add(ident)

        def work():
            try:
                task()
            finally:
                with self.jobs:
                    self.active.remove(ident)
                    self.slots.release()

        future = self.workers.submit(work)
        def report(done):
            failure = done.exception()
            if failure is not None:
                print(f"[DMB1] Trabajo interrumpido: {type(failure).__name__}", file=sys.stderr, flush=True)
        future.add_done_callback(report)
        return True

    def running(self, ident):
        with self.jobs:
            return ident in self.active

    def state(self, gateway, digest, minimum, conversation=None, attachment=None, secure=None):
        view = gateway.authorized_view(digest, minimum)
        actor = view.actor(digest)
        if attachment is not None:
            message = view.messages.get(attachment)
            require(message is not None and "attachment" in message, "FORBIDDEN")
            conversation = message["conversation"]
        conv = view.allowed(actor, conversation)
        if attachment is None:
            require(bool(conv.get("e2ee")) == bool(secure), "E2EE_REQUIRED" if conv.get("e2ee") else "E2EE_NOT_ENABLED")
            if secure:
                view.secure_message(conv, actor, digest, secure["device"], secure["epoch"], secure["envelope"])
        return (actor if attachment is None else message["attachment"]), view.cursor

    def load(self, transfer, owner):
        target = self.uploads / transfer
        try:
            record = json.loads((target / "state.json").read_text(encoding="utf-8"))
        except FileNotFoundError as error:
            raise Rejected("UNKNOWN_TRANSFER") from error
        require(record["session"] == owner, "UNKNOWN_TRANSFER")
        return target, record

    def save(self, path, details):
        temporary = path / "state.tmp"
        with temporary.open("w", encoding="utf-8") as stream:
            json.dump(details, stream, ensure_ascii=True, separators=(",", ":"))
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path / "state.json")
        sync_dir(path)

    def replica(self, name, operation, item, offset=0, data=b""):
        return remote(self.nodes[name], self.secret, name, operation,
                      item["sha256"], item["size"], offset, data)

    def status_receipt(self, name, item, operation=STATUS):
        raw = self.replica(name, operation, item)
        require(len(raw) in (9, 41), "INVALID_RECEIPT")
        sealed, offset = struct.unpack(">BQ", raw[:9])
        require((sealed == 0 and len(raw) == 9) or (
            sealed == 1 and len(raw) == 41 and offset == item["size"]), "INVALID_RECEIPT")
        return sealed, offset, raw[9:].hex()

    def copy(self, name, item, source):
        sealed, offset, signed = self.status_receipt(name, item)
        if sealed:
            return signed
        require(offset <= item["size"] and (offset == item["size"] or offset % BLOCK == 0), "INVALID_RECEIPT")
        with source.open("rb") as stream:
            stream.seek(offset)
            while offset < item["size"]:
                chunk = stream.read(min(BLOCK, item["size"] - offset))
                require(len(chunk) == min(BLOCK, item["size"] - offset), "INCOMPLETE_SOURCE")
                receipt, = struct.unpack(">Q", self.replica(name, PUT, item, offset, chunk))
                require(receipt == offset + len(chunk), "INVALID_RECEIPT")
                offset = receipt
        sealed, _, signed = self.status_receipt(name, item, SEAL)
        require(sealed == 1, "INVALID_RECEIPT")
        return signed

    def replicate(self, item, source):
        ready = {}
        for node in self.nodes:
            try:
                ready[node] = self.copy(node, item, source)
            except (OSError, EOFError, ValueError, struct.error) as error:
                reason = str(error) if isinstance(error, ValueError) else type(error).__name__
                print(f"[DMB1] Réplica {node} no verificada: {reason}", file=sys.stderr, flush=True)
                continue
        return ready

    def publish(self, gateway, path, item):
        sha, size = item["sha256"], item["size"]
        store = BlobStore(path)
        _, source = store.paths(sha)
        try:
            store.apply(SEAL, sha, size)
            encrypted = "envelope" in item
            if encrypted:
                with source.open("rb") as stream:
                    header = stream.read(20)
                require(len(header) == 20 and header[:4] == b"DGF1", "CONTENT_TYPE_MISMATCH")
                plain = int.from_bytes(header[4:12], "big")
                require(plain > 0 and 20 + plain + 16 * ((plain + 8191) // 8192) == size, "CONTENT_TYPE_MISMATCH")
                content_type = "application/octet-stream"
            else:
                content_type = detected_type(source)
                require(item["content_type"] == content_type or item["content_type"] == "application/octet-stream",
                        "CONTENT_TYPE_MISMATCH")
            replicas = self.replicate(item, source)
            item["replicas"] = sorted(replicas)
            if not item.get("proofs"):
                if len(replicas) < self.quorum:
                    item.update(status="PENDING", reason="INSUFFICIENT_DURABILITY")
                    return
                item["proofs"] = ",".join(f"{node}:{mac}" for node, mac in sorted(replicas.items()))
                with self.lock(path.name):
                    self.save(path, item)
            # Los recibos y el ID quedan fijos al reintentar la misma decisión.
            request = item["publication"]
            if len(replicas) < self.quorum:
                item.update(status="PENDING", reason="INSUFFICIENT_DURABILITY")
                return
            media_type = "IMAGE" if content_type.startswith("image/") else "FILE"
            fields = (item["session"], item["conversation"], request, "E2EE_FILE" if encrypted else media_type,
                      item["filename"], content_type, str(size), sha, item["proofs"])
            if encrypted:
                fields += (item["device"], item["epoch"], item["envelope"])
            command = gateway.chat.operation(SECURE_ATTACH if encrypted else ATTACH, *fields,
                                             token=bytes.fromhex(request))
            outcome = gateway.chat.submit(command)
            item.update(status=result_status(outcome), outcome=outcome, reason=None)
        except (OSError, ValueError) as error:
            code = str(error) if isinstance(error, ValueError) else "STORAGE_ERROR"
            status = "REJECTED" if code in {"CONTENT_TYPE_MISMATCH", "HASH_MISMATCH"} else "FAILED"
            item.update(status=status, reason=code)
        finally:
            with self.lock(path.name):
                self.save(path, item)
                if item["status"] in TERMINAL:
                    source.unlink(missing_ok=True)
                    store.paths(sha)[0].unlink(missing_ok=True)

    def remember(self, sha, path):
        hashes = []
        whole = hashlib.sha256()
        with path.open("rb") as stream:
            while block := stream.read(BLOCK):
                hashes.append(hashlib.sha256(block).digest())
                whole.update(block)
        require(whole.hexdigest() == sha, "HASH_MISMATCH")
        with self.verification:
            self.verified[sha] = (hashes, time.monotonic())
            self.verified.move_to_end(sha)
            if len(self.verified) > 128:
                self.verified.popitem(last=False)

    def recover(self, item):
        sha, size = item["sha256"], item["size"]
        part, found = self.cache.paths(sha)
        try:
            self.cache.apply(SEAL, sha, size)
        except (OSError, ValueError):
            for node in self.nodes:
                try:
                    require(self.status_receipt(node, item)[0] == 1, "MISSING_BLOB")
                    current = part.stat().st_size if part.exists() else 0
                    for offset in range(current, size, BLOCK):
                        block = self.replica(node, GET, item, offset)
                        self.cache.apply(PUT, sha, size, offset, block)
                    self.cache.apply(SEAL, sha, size)
                    break
                except (OSError, EOFError, ValueError, struct.error) as error:
                    reason = str(error) if isinstance(error, Rejected) else type(error).__name__
                    print(f"[DMB1] Recuperación {node}: {reason}", file=sys.stderr, flush=True)
                    part.unlink(missing_ok=True)
                    continue
            else:
                raise Rejected("CONTENT_UNAVAILABLE")
        self.remember(sha, found)
        self.replicate(item, found)

    def response(self, item, running=False):
        public = {name: item[name] for name in ("transfer", "filename", "size", "sha256")}
        public.update(status="REPLICATING" if running else item["status"], block_size=BLOCK)
        for name in ("outcome", "reason", "replicas"):
            if item.get(name) is not None:
                public[name] = item[name]
        return public

    def execute(self, gateway, op, data, rid, owner, minimum):
        require(set(data) == OPERATIONS[op], "INVALID_DATA")
        for name in ("conversation", "transfer", "attachment", "message", "device"):
            if name in data:
                require(isinstance(data[name], str) and ID.fullmatch(data[name]), "INVALID_ID")
        offset = data.get("offset", 0)
        require(type(offset) is int and 0 <= offset < MAX_FILE and offset % BLOCK == 0,
                "INVALID_OFFSET")
        if op in {"download-init", "download-chunk", "attachment-status"}:
            item, cursor = self.state(gateway, owner, minimum, attachment=data["attachment"])
            sha, size = item["sha256"], item["size"]
            _, path = self.cache.paths(sha)
            if op == "attachment-status":
                return {**item, "status": "COMMITTED"}, cursor
            with self.verification:
                verified = self.verified.get(sha)
            if op == "download-init":
                ready = verified is not None and path.is_file() and path.stat().st_size == size
                if (not ready or time.monotonic() - verified[1] > 10) and not self.running(sha):
                    def download():
                        try:
                            self.recover(item)
                        except (Rejected, OSError):
                            return
                    self.start(sha, download)
                return {**item, "block_size": BLOCK, "status": "READY" if ready else
                        "REPLICATING" if self.running(sha) else "PENDING"}, cursor
            try:
                require(verified is not None and offset < size, "DOWNLOAD_REQUIRES_READY")
                block = self.cache.apply(GET, sha, size, offset)
                require(hashlib.sha256(block).digest() == verified[0][offset // BLOCK], "HASH_MISMATCH")
            except (OSError, ValueError) as error:
                with self.verification:
                    self.verified.pop(sha, None)
                raise Rejected("DOWNLOAD_REQUIRES_READY") from error
            return {"block": base64.b64encode(block).decode("ascii"), "offset": offset}, cursor
        if op in {"upload-init", "upload-encrypted-init"}:
            encrypted = op == "upload-encrypted-init"
            if encrypted:
                require(isinstance(data["epoch"], str) and HASH.fullmatch(data["epoch"])
                        and isinstance(data["envelope"], str) and data["envelope"].isascii()
                        and len(data["envelope"]) <= 8192, "INVALID_DATA")
                data = {**data, "filename": "adjunto.dgf", "content_type": "application/octet-stream"}
            require(type(data["size"]) is int and 0 < data["size"] <= MAX_FILE, "INVALID_SIZE")
            require(isinstance(data["filename"], str) and valid_filename(data["filename"]), "INVALID_FILENAME")
            require(isinstance(data["sha256"], str) and HASH.fullmatch(data["sha256"]), "INVALID_HASH")
            require(isinstance(data["content_type"], str) and data["content_type"] in TYPES, "INVALID_TYPE")
            user, cursor = self.state(gateway, owner, minimum, conversation=data["conversation"],
                                      secure=data if encrypted else None)
            path = self.uploads / rid
            with self.catalog, self.lock(rid):
                if (path / "state.json").is_file():
                    _, item = self.load(rid, owner)
                    require(all(item[k] == v for k, v in data.items()), "TRANSFER_CONFLICT")
                else:
                    active = 0
                    for other in self.uploads.glob("*/state.json"):
                        with other.open(encoding="utf-8") as stream:
                            active += json.load(stream)["status"] not in TERMINAL
                        require(active < 32, "UPLOAD_LIMIT")
                    if not path.exists():
                        path.mkdir()
                        sync_dir(self.uploads)
                    item = {**data, "transfer": rid, "session": owner, "user": user, "status": "UPLOADING"}
                    self.save(path, item)
                return self.response(item, self.running(rid)), cursor
        transfer = data["transfer"]
        with self.lock(transfer):
            path, item = self.load(transfer, owner)
            running = self.running(transfer)
            if op == "upload-status":
                partial, final = BlobStore(path).paths(item["sha256"])
                received = final.stat().st_size if final.exists() else partial.stat().st_size if partial.exists() else 0
                if item["status"] == "COMMITTED":
                    received = item["size"]
                return {**self.response(item, running), "received": received}, 0
            if op == "upload-cancel":
                require(not running and "publication" not in item, "PUBLICATION_STARTED")
                for file in BlobStore(path).paths(item["sha256"]):
                    file.unlink(missing_ok=True)
                item["status"] = "CANCELLED"
                self.save(path, item)
                return self.response(item), 0
            if op == "upload-chunk":
                require(item["status"] == "UPLOADING" and not running, "UPLOAD_CLOSED")
                encoded = data["block"]
                require(isinstance(encoded, str) and len(encoded) <= 4 * ((BLOCK + 2) // 3), "INVALID_BLOCK")
                try:
                    raw = base64.b64decode(encoded, validate=True)
                    current, = struct.unpack(">Q", BlobStore(path).apply(
                        PUT, item["sha256"], item["size"], offset, raw))
                except ValueError as error:
                    code = str(error) if "-" not in str(error) and str(error).isupper() else "INVALID_BLOCK"
                    raise Rejected(code) from error
                return {**self.response(item), "received": current}, 0
            require(item["status"] != "CANCELLED", "UPLOAD_CLOSED")
            require(item.get("publication", rid) == rid and item.get("message", rid) == rid, "PUBLICATION_CONFLICT")
            if running or item["status"] in TERMINAL:
                cursor = item.get("outcome", {}).get("position", 0) if item["status"] == "COMMITTED" else 0
                return self.response(item, running), cursor
            partial, final = BlobStore(path).paths(item["sha256"])
            require(final.exists() or (partial.exists() and partial.stat().st_size == item["size"]),
                    "INCOMPLETE_BLOB")
            # Solo se adquiere el lock tras la barrera de autorización de abajo.
            conversation = item["conversation"]
        _, cursor = self.state(gateway, owner, minimum, conversation=conversation,
                               secure=item if "envelope" in item else None)
        with self.lock(transfer):
            path, item = self.load(transfer, owner)
            require(item["status"] != "CANCELLED" and item.get("publication", rid) == rid
                    and item.get("message", rid) == rid, "PUBLICATION_CONFLICT")
            if self.running(transfer) or item["status"] in TERMINAL:
                return self.response(item, self.running(transfer)), cursor
            item["publication"] = rid
            self.save(path, item)
            started = self.start(transfer, lambda: self.publish(gateway, path, dict(item)))
            return self.response(item, started), cursor
