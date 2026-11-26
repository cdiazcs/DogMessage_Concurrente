"""Motor 2PC con coordinador fijo, instantáneas durables y un trabajador de recuperación."""
from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import uuid

import two_pc_wire as w
import chat_feed
from media_store import admits_attachment


@dataclass(frozen=True)
class Record:
    tx: w.Tx
    decision: int = w.UNKNOWN
    votes: tuple = ()
    acked: frozenset = frozenset()


class Store:
    _open = set()
    _guard = threading.Lock()

    def __init__(self, directory, node, coordinator, members):
        self.directory = Path(directory).resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        self.file = self.directory / "two-pc-python.json"
        if any((self.directory / name).exists() for name in
               ("raft_state.json", "raft-state.bin", "term-state.bin", "raft.lock",
                "kotlin-raft.lock", "two-pc-jvm.bin")):
            raise ValueError("2PC exige un directorio separado para este motor")
        self.identity = [node, coordinator, sorted(members)]
        self.lock = None
        with self._guard:
            w.check(self.directory not in self._open, "Directorio 2PC en uso")
            self._open.add(self.directory)
        self.owned = True
        try:
            self.lock = (self.directory / "twopc.lock").open("a+b")
            self.lock.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.lockf(self.lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.rows = self.load()
        except Exception:
            self.close()
            raise

    @staticmethod
    def serialized(value):
        return json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode("ascii")

    def load(self):
        try:
            wrapper = json.loads(self.file.read_bytes())
        except FileNotFoundError:
            return ()
        payload = wrapper["payload"]
        w.check(hashlib.sha256(self.serialized(payload)).hexdigest() == wrapper["sha256"], "Checksum 2PC incorrecto")
        w.check(payload[:2] == ["2PC1", self.identity], "Identidad, membresía o versión 2PC distinta")
        rows = []
        identities = set()
        ids = set()
        members = set(self.identity[2]) - {self.identity[1]}
        for sequence, (encoded, decision, votes, acked) in enumerate(payload[2], 1):
            tx = w.read_payload(w.PREPARE, w.Reader(bytes.fromhex(encoded)))
            key = (tx.client, tx.request_id)
            w.check(tx.sequence == sequence and tx.coordinator == self.identity[1] and key not in identities
                    and tx.xid not in ids and decision in (0, 1, 2))
            w.check(set(acked) <= members and all(peer in members and vote in (0, 1) for peer, vote in votes)
                    and len({peer for peer, _ in votes}) == len(votes))
            w.check(decision != w.UNKNOWN or not acked)
            if rows:
                w.check(rows[-1].decision != w.UNKNOWN)
            if self.identity[0] == self.identity[1]:
                if rows:
                    w.check(rows[-1].acked == members)
                if decision == w.DECIDED_COMMIT:
                    w.check(dict(votes) == dict.fromkeys(members, 1))
            else:
                w.check(not votes and not acked)
            identities.add(key)
            ids.add(tx.xid)
            rows.append(Record(tx, decision, tuple(map(tuple, votes)), frozenset(acked)))
        return tuple(rows)

    def save(self, rows):
        payload = ["2PC1", self.identity, [[r.tx.encode().hex(), r.decision, r.votes, sorted(r.acked)] for r in rows]]
        wrapper = {"payload": payload, "sha256": hashlib.sha256(self.serialized(payload)).hexdigest()}
        fd, temporary = tempfile.mkstemp(dir=self.directory, prefix="twopc-", suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(self.serialized(wrapper))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.file)
            if os.name != "nt":
                directory = os.open(self.directory, os.O_RDONLY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
        finally:
            Path(temporary).unlink(missing_ok=True)
        self.rows = rows

    def close(self):
        with self._guard:
            if self.owned:
                if self.lock is not None:
                    self.lock.close()
                    self.lock = None
                self._open.discard(self.directory)
                self.owned = False


class Engine:
    def __init__(self, node, coordinator, peers, members, store, exchange):
        self.node, self.coordinator = node, coordinator
        self.peers, self.members = dict(peers), set(members)
        self.store, self.exchange = store, exchange
        self.lock = threading.RLock()
        self.stopped = threading.Event()
        self.wake = threading.Event()
        self.worker = None
        if self.is_coordinator and self.active and self.active.decision == w.UNKNOWN:
            self.update(replace(self.active, decision=w.DECIDED_ABORT))

    @property
    def is_coordinator(self):
        return self.node == self.coordinator

    @property
    def active(self):
        return self.store.rows[-1] if self.store.rows else None

    def update(self, row):
        rows = self.store.rows
        if row.tx.sequence == len(rows) + 1:
            next_rows = rows + (row,)
        else:
            w.check(row.tx.sequence == len(rows))
            next_rows = rows[:-1] + (row,)
        try:
            self.store.save(next_rows)
        except OSError as error:
            self.stopped.set()
            self.wake.set()
            print(f"[2PC {self.node}] ERROR persistencia: {error!r}", flush=True)
            raise

    def start(self):
        self.worker = threading.Thread(target=self.run, name=f"2pc-{self.node}", daemon=True)
        self.worker.start()

    def close(self):
        self.stopped.set()
        self.wake.set()
        if self.worker is not None:
            self.worker.join()

    def run(self):
        while not self.stopped.is_set():
            self.wake.wait(.15)
            self.wake.clear()
            try:
                self.tick()
            except OSError as error:
                self.stopped.set()
                print(f"[2PC {self.node}] ERROR {error}", flush=True)

    def call(self, peer, kind, tx):
        token = uuid.uuid4().bytes
        ref = (tx.sequence, tx.xid)
        payload = w.position(tx.sequence) + tx.xid if kind == w.QUERY else tx.encode()
        try:
            body = self.exchange(self.peers[peer], w.encode(kind, token, self.node, payload))
            return w.correlate(body, peer, token, kind, ref)[2]
        except (OSError, ValueError):
            return None

    def tick(self):
        with self.lock:
            if self.stopped.is_set():
                return
            row = self.active
        if not row:
            return
        if not self.is_coordinator:
            if row.decision == w.UNKNOWN:
                decision = self.call(self.coordinator, w.QUERY, row.tx)
                if decision in (w.DECIDED_COMMIT, w.DECIDED_ABORT):
                    with self.lock:
                        if not self.stopped.is_set():
                            self.participant(w.COMMIT if decision == 1 else w.ABORT, row.tx)
            return
        if row.decision == w.UNKNOWN:
            for peer in self.peers:
                vote = self.call(peer, w.PREPARE, row.tx)
                with self.lock:
                    if self.stopped.is_set():
                        return
                    if vote is not None:
                        row = replace(row, votes=row.votes + ((peer, vote),))
                        self.update(row)
                if vote != 1:
                    break
            with self.lock:
                if self.stopped.is_set():
                    return
                decision = w.DECIDED_COMMIT if dict(row.votes) == dict.fromkeys(self.peers, 1) else w.DECIDED_ABORT
                row = replace(row, decision=decision)
                self.update(row)
                print(f"[2PC {self.node}] decision={decision} sequence={row.tx.sequence}", flush=True)
        for peer in self.peers.keys() - row.acked:
            if self.stopped.is_set():
                return
            if self.call(peer, w.COMMIT if row.decision == 1 else w.ABORT, row.tx) == 1:
                with self.lock:
                    if self.stopped.is_set():
                        return
                    row = replace(row, acked=row.acked | {peer})
                    self.update(row)

    def submit(self, client, token, command):
        if not self.is_coordinator:
            return w.NOT_COORDINATOR, 0
        for row in self.store.rows:
            if (row.tx.client, row.tx.request_id) == (client, token):
                if row.tx.command != command:
                    return w.CONFLICT, 0
                if row.decision and row.acked == self.peers.keys():
                    return (w.COMMITTED if row.decision == 1 else w.ABORTED), row.tx.sequence
                return w.PENDING, row.tx.sequence
        if self.active and (not self.active.decision or self.active.acked != self.peers.keys()):
            return w.PENDING, 0
        if not admits_attachment(command, set(self.peers) | {self.node}, token, client):
            return w.CONFLICT, 0
        tx = w.Tx(len(self.store.rows) + 1, uuid.uuid4().bytes, self.coordinator, client, token, command)
        self.update(Record(tx))
        self.wake.set()
        return w.PENDING, tx.sequence

    def participant(self, kind, tx):
        rows = self.store.rows
        if tx.sequence <= len(rows):
            row = rows[tx.sequence - 1]
            if row.tx != tx:
                return 0
            if kind == w.PREPARE:
                return int(row.decision != w.DECIDED_ABORT)
            decision = w.DECIDED_COMMIT if kind == w.COMMIT else w.DECIDED_ABORT
            if row.decision:
                return int(row.decision == decision)
            self.update(replace(row, decision=decision))
            return 1
        if (tx.sequence != len(rows) + 1 or (self.active and not self.active.decision)
                or kind == w.COMMIT or any((r.tx.client, r.tx.request_id) == (tx.client, tx.request_id)
                                           or r.tx.xid == tx.xid for r in rows)):
            return 0
        self.update(Record(tx, w.DECIDED_ABORT if kind == w.ABORT else w.UNKNOWN))
        return 1

    def chat_snapshot(self, after):
        with self.lock:
            cursor = len(self.store.rows)
            if self.active and self.active.decision == w.UNKNOWN:
                cursor -= 1
            return cursor, tuple((row.tx.sequence, row.tx.command)
                                 for row in self.store.rows[min(after, cursor):cursor]
                                 if row.decision == w.DECIDED_COMMIT)

    def dispatch(self, body):
        if getattr(self, "chat_key", None) is not None and chat_feed.is_feed(body):
            return chat_feed.dispatch(body, self.node, self.chat_key, self.chat_snapshot)
        if body.startswith(b"DM1|"):
            return self.diagnostic.dispatch(body)
        try:
            algorithm, kind, token, sender, reader = w.header(body)
        except ValueError:
            return None

        def reply(response_kind, payload):
            return w.encode(response_kind, token, self.node, payload, algorithm)

        if kind == 0 or kind >= 0x80:
            return None
        if algorithm != w.ALGORITHM:
            return reply(0xff, b"\x01")
        if kind not in range(w.SUBMIT, w.QUERY + 1):
            return reply(0xff, b"\x02")
        try:
            data = w.read_payload(kind, reader)
        except ValueError:
            return reply(0xff, b"\x03")
        if kind != w.SUBMIT and sender not in self.peers:
            return reply(0xff, b"\x04")
        with self.lock:
            if self.stopped.is_set():
                return None
            if kind == w.SUBMIT:
                status, sequence = self.submit(sender, token, data)
                return reply(0x81, bytes([status]) + w.position(sequence, True) + w.node_bytes(self.coordinator))
            if kind == w.QUERY:
                sequence, xid = data
                decision = w.UNKNOWN
                if self.is_coordinator and sequence <= len(self.store.rows):
                    row = self.store.rows[sequence - 1]
                    if row.tx.xid == xid:
                        decision = row.decision
                return reply(0x85, w.position(sequence) + xid + bytes([decision]))
            accepted = 0
            if not self.is_coordinator and sender == self.coordinator == data.coordinator:
                accepted = self.participant(kind, data)
            return reply(kind | 0x80, w.position(data.sequence) + data.xid + bytes([accepted]))


def run(args, members, peers, transport, chat_key=None):
    w.check(args.data_dir is not None and args.coordinator in members, "2PC requiere --coordinator y --data-dir")
    store = Store(args.data_dir, args.node_id, args.coordinator, members)
    engine = None
    try:
        if args.inspect:
            # Inspeccionar STARTED sin convertirlo en ABORT.
            for row in store.rows:
                tx = row.tx
                status = ("STARTED" if args.node_id == args.coordinator else "PREPARED", "COMMIT", "ABORT")[row.decision]
                print(f"sequence={tx.sequence} status={status} tx={uuid.UUID(bytes=tx.xid)} client={tx.client} "
                      f"requestId={uuid.UUID(bytes=tx.request_id)} command={chat_feed.diagnostic_bytes(tx.command)}")
            return 0
        client = transport.TcpClient(.55)
        engine = Engine(args.node_id, args.coordinator, peers, members, store, client.exchange)
        engine.chat_key = chat_key
        engine.diagnostic = transport.Dispatcher(args.node_id, members, transport.RpcHandler())
        with transport.TcpServer(args.host, args.port, engine) as server:
            print(f"[2PC {args.node_id}] LISTEN {args.host}:{server.port} coordinator={args.coordinator}", flush=True)
            engine.start()
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
        return 0
    finally:
        if engine is not None:
            engine.close()
        store.close()
