from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "nodes/python"))
from secondary_engine import Engine, Store
import two_pc_wire as w


class TwoPcEngineTest(unittest.TestCase):
    configuration = {"n1", "n2", "n3"}
    peers = {"n1": ("127.0.0.1", 1), "n3": ("127.0.0.1", 3)}

    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="2pc python ")
        self.addCleanup(temp.cleanup)
        self.directory = Path(temp.name)
        self.tx = w.Tx(1, uuid.uuid4().bytes, "n1", "client", uuid.uuid4().bytes, b"\x00\xff\x80")

    def open(self, node="n2", exchange=lambda *_: b""):
        return Store(self.directory, node, "n1", self.configuration)

    def peer(self, store):
        return Engine("n2", "n1", self.peers, self.configuration, store, lambda *_: b"")

    def rpc(self, engine, kind, tx, sender="n1"):
        correlation = uuid.uuid4().bytes
        result = engine.dispatch(w.encode(kind, correlation, sender, tx.encode()))
        return w.correlate(result, "n2", correlation, kind, (tx.sequence, tx.xid))[2]

    def test_prepare_and_commit_are_idempotent_across_restart(self):
        store = self.open()
        try:
            engine = self.peer(store)
            self.assertEqual(self.rpc(engine, w.COMMIT, self.tx), 0)
            self.assertEqual(self.rpc(engine, w.PREPARE, self.tx), 1)
            self.assertEqual(self.rpc(engine, w.PREPARE, self.tx), 1)
            with self.assertRaisesRegex(ValueError, "en uso"):
                self.open()
        finally:
            store.close()
        store = self.open()
        try:
            self.assertEqual(store.rows[0].decision, w.UNKNOWN)
            self.assertEqual(store.rows[0].tx, self.tx)
            engine = self.peer(store)
            for _ in range(2):
                self.assertEqual(self.rpc(engine, w.COMMIT, self.tx), 1)
            self.assertEqual(self.rpc(engine, w.ABORT, self.tx), 0)
        finally:
            store.close()
        store = self.open()
        try:
            self.assertEqual(store.rows[0].decision, w.DECIDED_COMMIT)
        finally:
            store.close()

    def test_abort_without_prepare_and_contradiction(self):
        store = self.open()
        try:
            engine = self.peer(store)
            self.assertEqual(self.rpc(engine, w.ABORT, self.tx), 1)
            self.assertEqual(self.rpc(engine, w.ABORT, self.tx), 1)
            self.assertEqual(self.rpc(engine, w.PREPARE, self.tx), 0)
            self.assertEqual(self.rpc(engine, w.COMMIT, self.tx), 0)
        finally:
            store.close()
        store = self.open()
        try:
            self.assertEqual(store.rows[0].decision, w.DECIDED_ABORT)
        finally:
            store.close()

    def test_write_failure_closes_without_yes(self):
        store = self.open()
        try:
            engine = self.peer(store)
            with patch("secondary_engine.os.replace", side_effect=OSError("escritura fallida")):
                with self.assertRaisesRegex(OSError, "escritura fallida"):
                    engine.dispatch(w.encode(w.PREPARE, uuid.uuid4().bytes, "n1", self.tx.encode()))
            self.assertIsNone(engine.dispatch(w.encode(w.PREPARE, uuid.uuid4().bytes, "n1", self.tx.encode())))
            self.assertEqual(store.rows, ())
        finally:
            store.close()

    def test_recovery_aborts_started_and_rejects_conflicting_bytes(self):
        store = self.open("n1")
        peers = {"n2": ("127.0.0.1", 2), "n3": ("127.0.0.1", 3)}
        try:
            engine = Engine("n1", "n1", peers, self.configuration, store, lambda *_: b"")
            self.assertEqual(engine.submit("client", self.tx.request_id, self.tx.command), (w.PENDING, 1))
            self.assertEqual(engine.submit("client", self.tx.request_id, b"otro"), (w.CONFLICT, 0))
        finally:
            store.close()
        store = self.open("n1")
        try:
            Engine("n1", "n1", peers, self.configuration, store, lambda *_: b"")
            self.assertEqual(store.rows[0].decision, w.DECIDED_ABORT)
        finally:
            store.close()

    def test_duplicate_vote_cannot_stand_in_for_other_participant(self):
        store = self.open("n1")
        peers = {"n2": ("127.0.0.1", 2), "n3": ("127.0.0.1", 3)}
        def exchange(endpoint, body):
            _, kind, correlation, _, reader = w.header(body)
            tx = w.read_payload(kind, reader)
            # n3 repite el voto de n2 con la correlación vigente.
            return w.encode(kind | 0x80, correlation, "n2", w.position(tx.sequence) + tx.xid + b"\x01")

        try:
            engine = Engine("n1", "n1", peers, self.configuration, store, exchange)
            engine.submit("client", self.tx.request_id, self.tx.command)
            engine.tick()
            self.assertEqual(store.rows[0].votes, (("n2", 1),))
            self.assertEqual(store.rows[0].decision, w.DECIDED_ABORT)
            self.assertNotIn("n3", store.rows[0].acked)
        finally:
            store.close()
