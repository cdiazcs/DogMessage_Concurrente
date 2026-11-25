"""Motor Raft Python sobre DM2 binario: reglas del contrato que afectan la interoperabilidad."""
import contextlib
import io
import json
import shutil
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "nodes" / "python"))
import dm2_codec as dm2  # noqa: E402
import node  # noqa: E402
import raft_engine  # noqa: E402
from storage import NodeStorage, StorageError  # noqa: E402

PEERS = {"n1": ("127.0.0.1", 1), "n3": ("127.0.0.1", 3)}
RID = bytes.fromhex("fedcba9876543210fedcba9876543210")


class OfflineClient:
    def call_rpc(self, *args):
        raise OSError("sin red en tests unitarios")


def header(sender, kind, request_id=None):
    return dm2.Header(dm2.RAFT, kind, request_id or node.new_request_id(), sender)


def command(term, i):
    return dm2.LogEntry.of_command(term, "c1", bytes([i + 1]) * 16, f"m{i}".encode())


class EngineCase(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(prefix="raft-py-"))
        self.addCleanup(shutil.rmtree, self.dir, True)
        self.out = io.StringIO()
        self.engine = self.make()

    def make(self, peers=PEERS):
        with contextlib.redirect_stdout(self.out):
            return raft_engine.RaftEngine("n2", peers, self.dir, OfflineClient())

    def vote(self, sender, term, last_index=0, last_term=0, engine=None):
        with contextlib.redirect_stdout(self.out):
            return (engine or self.engine).request_vote(header(sender, dm2.REQUEST_VOTE),
                                                       dm2.RequestVote(term, last_index, last_term))

    def append(self, term, prev_index, prev_term, commit, entries=(), sender="n1"):
        with contextlib.redirect_stdout(self.out):
            return self.engine.append_entries(header(sender, dm2.APPEND_ENTRIES),
                                              dm2.AppendEntries(term, prev_index, prev_term, commit, tuple(entries)))


class VoteTest(EngineCase):
    def test_grant_once_per_term_and_persist(self):
        self.assertEqual(self.vote("n1", 3), dm2.VoteResponse(3, True))
        self.assertEqual(self.vote("n3", 3), dm2.VoteResponse(3, False))
        self.assertEqual(self.vote("n1", 3), dm2.VoteResponse(3, True), "mismo candidato puede reintentar")
        restarted = self.make()
        self.assertEqual((restarted.current_term, restarted.voted_for), (3, "n1"))
        self.assertEqual(self.vote("n3", 3, engine=restarted), dm2.VoteResponse(3, False))

    def test_stale_term_and_outdated_log(self):
        self.vote("n1", 5)
        self.assertEqual(self.vote("n3", 4), dm2.VoteResponse(5, False))
        self.append(5, 0, 0, 0, [command(5, 0), command(5, 1)])
        self.assertEqual(self.vote("n3", 6, last_index=1, last_term=5), dm2.VoteResponse(6, False))
        self.assertEqual(self.engine.current_term, 6, "término superior avanza aunque se deniegue")
        self.assertEqual(self.vote("n3", 7, last_index=1, last_term=6), dm2.VoteResponse(7, True))


class AppendTest(EngineCase):
    def test_heartbeat_origin_and_mismatch(self):
        self.assertEqual(self.append(2, 0, 0, 0), dm2.AppendResponse(2, True, 0))
        self.assertEqual(self.append(2, 4, 2, 0), dm2.AppendResponse(2, False, 0))
        self.assertEqual(self.append(1, 0, 0, 0), dm2.AppendResponse(2, False, 0), "término obsoleto")
        self.assertEqual(self.engine.leader_id, "n1")

    def test_match_index_is_end_of_batch_not_log_length(self):
        """Java descarta un éxito cuyo matchIndex no sea prevLog.index + entryCount."""
        entries = [command(1, i) for i in range(5)]
        self.assertEqual(self.append(1, 0, 0, 0, entries), dm2.AppendResponse(1, True, 5))
        response = self.append(1, 1, 1, 5, [entries[1]])
        self.assertEqual(response, dm2.AppendResponse(1, True, 2))
        self.assertEqual(len(self.engine.log), 5, "sin conflicto se conserva el sufijo local")
        self.assertEqual(self.engine.commit_index, 2, "commit = min(leaderCommit, matchIndex)")

    def test_conflicting_suffix_is_replaced(self):
        self.append(1, 0, 0, 0, [command(1, i) for i in range(3)])
        self.append(2, 1, 1, 1, [command(2, 7)])
        self.assertEqual([e.term for e in self.engine.log], [1, 2])
        self.assertEqual(self.engine.log[1].command, b"m7")

    def test_commit_index_survives_restart(self):
        self.append(1, 0, 0, 0, [command(1, i) for i in range(4)])
        self.append(1, 4, 1, 2)
        restarted = self.make()
        self.assertEqual((restarted.commit_index, restarted.last_applied, len(restarted.log)), (2, 2, 4))
        self.assertEqual(restarted.log[3].command, b"m3")

    def test_unknown_member_does_not_touch_state(self):
        dispatcher = node.Dispatcher("n2", {"n1", "n2", "n3"}, self.engine)
        request = dm2.Message(header("n9", dm2.APPEND_ENTRIES), dm2.AppendEntries(9, 0, 0, 0))
        reply = dm2.decode(dispatcher.dispatch(dm2.encode(request)))
        self.assertEqual(reply.payload, dm2.ErrorMsg(dm2.UNKNOWN_MEMBER))
        self.assertEqual(self.engine.current_term, 0)


class ClientCommandTest(EngineCase):
    def test_follower_answers_not_leader_with_hint(self):
        self.append(3, 0, 0, 0)
        with contextlib.redirect_stdout(self.out):
            result = self.engine.client_command(header("c1", dm2.CLIENT_COMMAND), dm2.ClientCommand(b"x"))
        self.assertEqual(result, dm2.ClientResult(dm2.NOT_LEADER, 0, "n1"))
        self.assertEqual(self.engine.log, [], "sin reenvío interno ni escritura")

    def test_leader_deduplicates_and_detects_conflicts(self):
        self.engine = self.make(peers={})
        with contextlib.redirect_stdout(self.out):
            with self.engine.lock:
                self.engine._start_election()
            self.assertEqual(self.engine.state, "LEADER")
            first = self.engine.client_command(header("c1", dm2.CLIENT_COMMAND, RID), dm2.ClientCommand(b"\x00\xff\x80"))
            again = self.engine.client_command(header("c1", dm2.CLIENT_COMMAND, RID), dm2.ClientCommand(b"\x00\xff\x80"))
            other = self.engine.client_command(header("c2", dm2.CLIENT_COMMAND, RID), dm2.ClientCommand(b"y"))
            clash = self.engine.client_command(header("c1", dm2.CLIENT_COMMAND, RID), dm2.ClientCommand(b"z"))
        self.assertEqual(first, dm2.ClientResult(dm2.COMMITTED, 1))
        self.assertEqual(again, dm2.ClientResult(dm2.COMMITTED, 1))
        self.assertEqual(other, dm2.ClientResult(dm2.COMMITTED, 2), "la clave es (clientId, requestId)")
        self.assertEqual(clash, dm2.ClientResult(dm2.REQUEST_ID_CONFLICT, 0))
        self.assertEqual(len(self.engine.log), 2)

    def test_leader_batches_are_bounded(self):
        self.engine = self.make()
        self.engine.log = [dm2.LogEntry.no_op(1)] * 300
        self.engine.current_term = 1
        batch = self.engine._batch(1)
        self.assertEqual((batch.prev_log_index, len(batch.entries)), (0, dm2.MAX_APPEND_ENTRIES))
        self.assertEqual(self.engine._batch(290).prev_log_index, 289)


class StorageTest(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(prefix="raft-st-"))
        self.addCleanup(shutil.rmtree, self.dir, True)

    def test_corrupt_state_is_not_silently_reset(self):
        (self.dir / "raft_state.json").write_text("{no es json", encoding="utf-8")
        with self.assertRaises(StorageError):
            NodeStorage(self.dir, "n2").load()

    def test_other_node_state_is_rejected(self):
        engine = raft_engine.RaftEngine("n2", PEERS, self.dir, OfflineClient())
        with engine.lock, contextlib.redirect_stdout(io.StringIO()):
            engine._step_down(4)
        with self.assertRaises(StorageError):
            NodeStorage(self.dir, "n1").load()

    def test_inspect_matches_java_format(self):
        storage = NodeStorage(self.dir, "n2")
        from storage import PersistentState
        storage.save(PersistentState(4, None, 1, [dm2.LogEntry.no_op(4),
                                                  dm2.LogEntry.of_command(4, "c1", RID, b"\x00")]))
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(raft_engine.inspect(self.dir, "n2"), 0)
        self.assertEqual(out.getvalue().splitlines(), [
            "role=FOLLOWER term=4 leader=null commitIndex=1 lastApplied=1 lastLogIndex=2 appliedCommands=[]",
            "index=1 term=4 type=NO_OP status=COMMITTED client=null requestId=null command=-",
            "index=2 term=4 type=COMMAND status=PENDING client=c1 requestId=fedcba98-7654-3210-fedc-ba9876543210 command=00",
        ])


class RecordingClient:
    """Concede votos y acepta lotes; registra cada RPC saliente. Puede retener respuestas."""

    def __init__(self):
        self.calls = []
        self.release = None  # threading.Event opcional para retener la respuesta

    def call_rpc(self, endpoint, expected_peer, sender_id, payload):
        self.calls.append((expected_peer, type(payload).__name__))
        if self.release is not None:
            self.release.wait(5)
        if isinstance(payload, dm2.RequestVote):
            response = dm2.VoteResponse(payload.term, True)
        else:
            response = dm2.AppendResponse(payload.term, True, payload.last_index)
        return dm2.Message(dm2.Header(dm2.RAFT, response.TYPE, node.new_request_id(), expected_peer), response)


class IsolationTest(unittest.TestCase):
    """Partición simétrica por <data-dir>/fault.json: ni responde ni envía ni procesa respuestas."""

    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(prefix="raft-iso-"))
        self.addCleanup(shutil.rmtree, self.dir, True)
        self.client = RecordingClient()
        self.out = io.StringIO()
        with contextlib.redirect_stdout(self.out):
            self.engine = raft_engine.RaftEngine("n2", PEERS, self.dir, self.client)
        self.addCleanup(self.engine.stop, 2.0)

    def isolate(self, value):
        (self.dir / raft_engine.FAULT_FILE).write_text(json.dumps({"isolated": value}), encoding="utf-8")
        deadline = time.monotonic() + 2
        while self.engine.isolated != value and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(self.engine.isolated, value)

    def wait_for(self, condition, limit=3.0):
        deadline = time.monotonic() + limit
        while not condition() and time.monotonic() < deadline:
            time.sleep(0.01)
        return condition()

    def test_isolated_follower_neither_answers_nor_campaigns(self):
        with contextlib.redirect_stdout(self.out):
            self.engine.start()
            self.isolate(True)
            time.sleep(1.2)  # > plazo máximo de elección (0,70 s)
            vote = self.engine.request_vote(header("n1", dm2.REQUEST_VOTE), dm2.RequestVote(9, 0, 0))
            append = self.engine.append_entries(header("n1", dm2.APPEND_ENTRIES), dm2.AppendEntries(9, 0, 0, 0))
            result = self.engine.client_command(header("c1", dm2.CLIENT_COMMAND), dm2.ClientCommand(b"x"))
        self.assertEqual((vote, append, result), (None, None, None), "no responde")
        self.assertEqual(self.client.calls, [], "no envía RequestVote")
        self.assertEqual((self.engine.state, self.engine.current_term), ("FOLLOWER", 0))

    def test_isolated_leader_stops_sending_append_entries(self):
        with contextlib.redirect_stdout(self.out):
            self.engine.start()
            self.assertTrue(self.wait_for(lambda: self.engine.state == "LEADER"))
            self.assertTrue(self.wait_for(lambda: any(k == "AppendEntries" for _, k in self.client.calls)))
            self.isolate(True)
            time.sleep(0.2)  # deja terminar cualquier RPC ya en curso
            self.client.calls.clear()
            time.sleep(0.6)  # ~6 intervalos de heartbeat
        self.assertEqual(self.client.calls, [], "no envía AppendEntries")
        with contextlib.redirect_stdout(self.out):
            self.isolate(False)
            self.assertTrue(self.wait_for(lambda: any(k == "AppendEntries" for _, k in self.client.calls)),
                            "al sanar vuelve a replicar")

    def test_late_responses_are_ignored_while_isolated(self):
        self.client.release = threading.Event()
        with contextlib.redirect_stdout(self.out):
            self.engine.start()
            self.assertTrue(self.wait_for(lambda: len(self.client.calls) >= 2), "RequestVote en curso")
            self.isolate(True)
            self.client.release.set()  # llegan votos concedidos estando aislado
            time.sleep(0.3)
        self.assertNotEqual(self.engine.state, "LEADER", "no gana una elección con respuestas tardías")


if __name__ == "__main__":
    unittest.main()
