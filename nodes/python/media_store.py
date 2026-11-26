"""DMB1: almacén de bloques y servidor interno."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import hmac
import os
from pathlib import Path
import re
import shutil
import socket
import struct
from threading import BoundedSemaphore, Lock
import time
import uuid

BLOCK = 8192
MAX_FILE = 50 * 1024 * 1024
LIMIT = BLOCK + 128
MAGIC = b"DMB1"
STATUS, PUT, SEAL, GET = range(1, 5)
HEADER = struct.Struct(">4sB16s2s32sQQ")
HEX = re.compile(r"[0-9a-f]{64}")


def check(condition, reason="INVALID_BLOCK"):
    if not condition:
        raise ValueError(reason)


def key(variable="DOG_MEDIA_KEY"):
    value = os.environ.get(variable, "")
    check(HEX.fullmatch(value), variable + " debe ser hexadecimal de 32 bytes")
    return bytes.fromhex(value)


def proof(secret, node, digest, size):
    statement = b"DMB1-durable\0" + node.encode("ascii") + bytes.fromhex(digest) + struct.pack(">Q", size)
    return hmac.digest(secret, statement, "sha256")


def admits_attachment(raw, members, token, client):
    if not raw.startswith((b"CHAT1\x0a", b"CHAT1\x0f")):
        return True
    try:
        check(client == "chat-app" and raw[6:22] == token and len(raw) >= 54)
        envelope, signature = raw[:-32], raw[-32:]
        check(hmac.compare_digest(hmac.digest(key("DOG_CHAT_KEY"), envelope, "sha256"), signature))
        fields, offset = [], 22
        for _ in range(9 if raw[5] == 10 else 12):
            length, = struct.unpack_from(">H", envelope, offset)
            offset += 2
            check(offset + length <= len(envelope))
            fields.append(envelope[offset:offset + length].decode("utf-8"))
            offset += length
        check(offset == len(envelope) and fields[2] == token.hex())
        size, digest, receipts = int(fields[6]), fields[7], fields[8]
        check(HEX.fullmatch(digest) and 0 < size <= MAX_FILE)
        secret = key("DOG_MEDIA_RECEIPT_KEY")
        observed = set()
        for entry in receipts.split(","):
            node, mac = entry.split(":")
            check(node in members and node not in observed and HEX.fullmatch(mac))
            check(hmac.compare_digest(proof(secret, node, digest, size), bytes.fromhex(mac)))
            observed.add(node)
        return observed == members
    except (ValueError, struct.error):
        return False

def exact(conn, n):
    result = bytearray()
    deadline = time.monotonic() + 5
    while len(result) < n:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError()
        conn.settimeout(remaining)
        data = conn.recv(n - len(result))
        if not data:
            raise EOFError()
        result.extend(data)
    return bytes(result)


def frame(conn):
    length, = struct.unpack(">I", exact(conn, 4))
    check(HEADER.size + 32 <= length <= LIMIT)
    return exact(conn, length)


def pack(secret, op, request, node, digest, size, offset, payload=b""):
    body = HEADER.pack(MAGIC, op, request, node.encode("ascii"), bytes.fromhex(digest), size, offset) + payload
    wire = body + hmac.digest(secret, body, "sha256")
    return struct.pack(">I", len(wire)) + wire


def unpack(secret, raw):
    body, mac = raw[:-32], raw[-32:]
    check(hmac.compare_digest(hmac.digest(secret, body, "sha256"), mac), "INVALID_MAC")
    magic, op, request, node, digest, size, offset = HEADER.unpack_from(body)
    check(magic == MAGIC and request != bytes(16))
    return op, request, node.decode("ascii"), digest.hex(), size, offset, body[HEADER.size:]


def sync_dir(directory):
    if os.name == "nt":
        return
    descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@contextmanager
def directory_lock(directory):
    with (Path(directory) / ".dmb-lock").open("a+b") as stream:
        stream.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.lockf(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield

def file_hash(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


class BlobStore:
    def __init__(self, root):
        self.root = Path(root)
        if not self.root.exists():
            self.root.mkdir(parents=True, exist_ok=True)
            sync_dir(self.root.parent)
        self.locks = [Lock() for _ in range(64)]

    def paths(self, digest):
        check(HEX.fullmatch(digest))
        return self.root / (digest + ".part"), self.root / (digest + ".bin")

    def apply(self, op, digest, size, offset=0, data=b""):
        check(0 < size <= MAX_FILE and 0 <= offset <= size)
        part, final = self.paths(digest)
        with self.locks[int(digest[:2], 16) % len(self.locks)]:
            if op == GET:
                check(not data and offset < size and offset % BLOCK == 0 and final.is_file()
                      and final.stat().st_size == size, "MISSING_BLOB")
                with final.open("rb") as stream:
                    stream.seek(offset)
                    return stream.read(min(BLOCK, size - offset))
            current = part.stat().st_size if part.exists() else 0
            if 0 < current < size and current % BLOCK:
                current -= current % BLOCK
                with part.open("r+b") as stream:
                    stream.truncate(current)
                    stream.flush()
                    os.fsync(stream.fileno())
            if op in (STATUS, SEAL):
                check(not data and offset == 0)
                if final.is_file() and final.stat().st_size == size and file_hash(final) == digest:
                    sync_dir(self.root)
                    return struct.pack(">BQ", 1, size)
                check(current <= size and (current == size or current % BLOCK == 0), "INVALID_PART")
                if op == STATUS:
                    return struct.pack(">BQ", 0, current)
                check(current == size, "INCOMPLETE_BLOB")
                if file_hash(part) != digest:
                    part.unlink()
                    sync_dir(self.root)
                    raise ValueError("HASH_MISMATCH")
                part.replace(final)
                sync_dir(self.root)
                return struct.pack(">BQ", 1, size)
            check(op == PUT and offset < size and offset % BLOCK == 0 and len(data) == min(BLOCK, size - offset))
            # Una copia ya publicada puede ser corrupta. STATUS informa su estado
            # y PUT reconstruye un temporal que solo SEAL puede publicar.
            if offset < current:
                with part.open("rb") as stream:
                    stream.seek(offset)
                    check(stream.read(len(data)) == data, "BLOCK_CONFLICT")
                return struct.pack(">Q", current)
            check(offset == current, "OUT_OF_ORDER")
            check(shutil.disk_usage(self.root).free >= len(data) + BLOCK, "NO_SPACE")
            with part.open("r+b" if part.exists() else "w+b") as stream:
                stream.seek(offset)
                try:
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
                except OSError:
                    stream.truncate(current)
                    raise
            sync_dir(self.root)
            return struct.pack(">Q", current + len(data))


def serve(store, node, host, port, secret, receipts, workers=6):
    slots = BoundedSemaphore(workers)

    def handle(conn):
        try:
            with conn:
                op, request, target, digest, size, offset, data = unpack(secret, frame(conn))
                check(target == node and op in (STATUS, PUT, SEAL, GET))
                try:
                    payload = store.apply(op, digest, size, offset, data)
                    if op in (STATUS, SEAL) and payload[0] == 1:
                        payload += proof(receipts, node, digest, size)
                    result = b"\0" + payload
                except ValueError as error:
                    result = b"\1" + str(error).encode("ascii")
                except OSError:
                    result = b"\1STORAGE_ERROR"
                conn.settimeout(5)
                conn.sendall(pack(secret, op | 128, request, node, digest, size, offset, result))
        except (OSError, EOFError, ValueError):
            pass
        finally:
            slots.release()

    with socket.socket() as server, ThreadPoolExecutor(max_workers=workers) as threads:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((host, port))
        server.listen(workers)
        print(f"DMB1 {node} {host}:{port}", flush=True)
        while True:
            conn, _ = server.accept()
            if slots.acquire(blocking=False):
                threads.submit(handle, conn)
            else:
                conn.close()


def remote(endpoint, secret, node, op, digest, size, offset=0, data=b""):
    request = uuid.uuid4().bytes
    with socket.create_connection(endpoint, timeout=2) as conn:
        conn.settimeout(5)
        conn.sendall(pack(secret, op, request, node, digest, size, offset, data))
        reply = unpack(secret, frame(conn))
    check(reply[:6] == (op | 128, request, node, digest, size, offset), "INVALID_REPLY")
    payload = reply[-1]
    check(bool(payload), "INVALID_REPLY")
    if payload[0] != 0:
        raise ValueError(payload[1:].decode("ascii", errors="replace"))
    return payload[1:]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node", required=True, choices=("n1", "n2", "n3"))
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--data-dir", required=True, type=Path)
    args = parser.parse_args()
    secret, receipts = key(), key("DOG_MEDIA_RECEIPT_KEY")
    check(secret != receipts, "Las claves DMB1 y de recibos deben ser distintas")
    store = BlobStore(args.data_dir)
    with directory_lock(store.root):
        serve(store, args.node, args.host, args.port, secret, receipts)


if __name__ == "__main__":
    main()
