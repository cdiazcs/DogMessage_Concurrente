"""Motor Raft Python sobre DM2 binario: reglas del contrato que afectan la interoperabilidad."""
import contextlib
import io
import json
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "nodes" / "python"))
import dm2_codec as dm2  # noqa: E402
import node  # noqa: E402
import raft_engine  # noqa: E402
from storage import NodeStorage, PersistentState, StorageError  # noqa: E402

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
        if hasattr(self, "engine"):
            self.engine.stop()
        with contextlib.redirect_stdout(self.out):
            engine = raft_engine.RaftEngine("n2", peers, self.dir, OfflineClient())
        self.addCleanup(engine.stop, 1.0)
        return engine

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

    def test_same_term_append_preserves_candidate_vote_after_restart(self):
        with self.engine.lock, contextlib.redirect_stdout(self.out):
            self.engine._start_election()
        self.assertEqual(self.engine.state, "CANDIDATE")
        self.assertEqual(self.append(1, 9, 1, 0), dm2.AppendResponse(1, False, 0))
        self.assertEqual((self.engine.state, self.engine.voted_for), ("FOLLOWER", "n2"))
        self.assertEqual(self.vote("n3", 1), dm2.VoteResponse(1, False))
        restarted = self.make()
        self.assertEqual((restarted.current_term, restarted.voted_for), (1, "n2"))
        self.assertEqual(self.vote("n3", 1, engine=restarted), dm2.VoteResponse(1, False))
        self.assertEqual(self.vote("n3", 2, engine=restarted), dm2.VoteResponse(2, True))

    def test_failed_vote_write_does_not_reply_or_erase_previous_vote(self):
        self.assertEqual(self.vote("n1", 3), dm2.VoteResponse(3, True))
        with patch("storage.os.replace", side_effect=OSError("fallo de reemplazo")), \
                contextlib.redirect_stderr(self.out):
            with self.assertRaisesRegex(OSError, "fallo de reemplazo"):
                self.vote("n3", 4)
        self.assertIn("stopped: fallo de reemplazo", self.out.getvalue())
        with self.assertRaisesRegex(OSError, "Raft detenido"):
            self.vote("n1", 3)
        restarted = self.make()
        self.assertEqual((restarted.current_term, restarted.voted_for), (3, "n1"))

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

    def test_prev_log_mismatch_leaves_even_uncommitted_suffix_intact(self):
        old = [command(1, i) for i in range(3)]
        self.append(1, 0, 0, 1, old)
        self.assertEqual(self.append(2, 2, 2, 0), dm2.AppendResponse(2, False, 0))
        self.assertEqual(self.engine.log, old)
        self.assertEqual(self.make().log, old)

    def test_conflicting_committed_entry_stops_engine_and_preserves_log(self):
        old = [command(1, 0)]
        self.append(1, 0, 0, 1, old)
        with contextlib.redirect_stderr(self.out):
            with self.assertRaisesRegex(OSError, "prefijo comprometido"):
                self.append(2, 0, 0, 0, [command(2, 0)])
        with self.assertRaisesRegex(OSError, "Raft detenido"):
            self.append(2, 0, 0, 0)
        restarted = self.make()
        self.assertEqual((restarted.commit_index, restarted.log), (1, old))

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
        self.assertEqual(first, dm2.ClientResult(dm2.COMMITTED, 2))
        self.assertEqual(again, dm2.ClientResult(dm2.COMMITTED, 2))
        self.assertEqual(other, dm2.ClientResult(dm2.COMMITTED, 3), "la clave es (clientId, requestId)")
        self.assertEqual(clash, dm2.ClientResult(dm2.REQUEST_ID_CONFLICT, 0))
        self.assertEqual(len(self.engine.log), 3)

    def test_leader_batches_are_bounded(self):
        self.engine = self.make()
        self.engine.log = [dm2.LogEntry.no_op(1)] * 300
        self.engine.current_term = 1
        batch = self.engine._batch(1)
        self.assertEqual((batch.prev_log_index, len(batch.entries)), (0, dm2.MAX_APPEND_ENTRIES))
        self.assertEqual(self.engine._batch(290).prev_log_index, 289)


class LeadershipRegressionTest(EngineCase):
    def test_quorum_must_confirm_current_term_no_op_to_commit_old_prefix(self):
        old = command(1, 0)
        self.append(1, 0, 0, 0, [old] + [dm2.LogEntry.no_op(1)] * 255)
        engine = self.engine
        with engine.lock, patch.object(engine, "_send"), contextlib.redirect_stdout(self.out):
            engine._start_election()
            vote = dm2.RequestVote(2, 256, 1)
            engine._accept_response("n1", vote, "CANDIDATE", dm2.VoteResponse(2, True))
            self.assertEqual(engine.log[-1], dm2.LogEntry.no_op(2))
            self.assertEqual(engine.next_index["n1"], 257)
            probe = engine._batch(257)
            engine._accept_response("n1", probe, "LEADER", dm2.AppendResponse(2, False, 0))
            prefix = engine._batch(1)
            self.assertEqual(prefix.last_index, 256)
            engine._accept_response("n1", prefix, "LEADER", dm2.AppendResponse(2, True, 256))
            self.assertEqual((engine.commit_index, engine.last_applied), (0, 0))
            no_op = engine._batch(engine.next_index["n1"])
            self.assertEqual(no_op.entries, (dm2.LogEntry.no_op(2),))
            engine._accept_response("n1", no_op, "LEADER", dm2.AppendResponse(2, True, 257))
        restarted = self.make()
        self.assertEqual((restarted.commit_index, restarted.last_applied), (257, 257))
        self.assertEqual(restarted.applied_requests, {(old.client_id, old.client_request_id): 1})
        self.assertEqual(restarted.metrics()["commands_committed"], 1)

    def test_single_replica_commits_no_op_after_election_and_restart(self):
        engine = self.make(peers={})
        self.engine = engine
        with engine.lock, contextlib.redirect_stdout(self.out):
            engine._start_election()
        restarted = self.make(peers={})
        self.assertEqual(restarted.log, [dm2.LogEntry.no_op(1)])
        self.assertEqual((restarted.commit_index, restarted.last_applied), (1, 1))
        self.assertEqual(restarted.applied_requests, {})

    def test_leader_rejects_another_leader_claim_in_same_term(self):
        engine = self.engine
        with engine.lock, patch.object(engine, "_send"), contextlib.redirect_stdout(self.out):
            engine._start_election()
            engine._accept_response("n1", dm2.RequestVote(1, 0, 0), "CANDIDATE", dm2.VoteResponse(1, True))
        self.assertEqual(self.append(1, 0, 0, 0, sender="n3"), dm2.AppendResponse(1, False, 0))
        self.assertEqual((engine.state, engine.leader_id, engine.voted_for), ("LEADER", "n2", "n2"))

    def test_failed_commit_does_not_acknowledge_waiting_client(self):
        engine = self.engine
        waiting = threading.Event()
        responses = []
        wait = engine._commit_cond.wait

        def wait_for_commit(timeout):
            waiting.set()
            return wait(timeout)

        def propose():
            responses.append(engine.client_command(header("c1", dm2.CLIENT_COMMAND, RID),
                                                   dm2.ClientCommand(b"mensaje")))

        thread = threading.Thread(target=propose)
        with patch.object(engine, "_send"), patch.object(engine._commit_cond, "wait", side_effect=wait_for_commit), \
                contextlib.redirect_stdout(self.out), contextlib.redirect_stderr(self.out):
            with engine.lock:
                engine._start_election()
                engine._accept_response("n1", dm2.RequestVote(1, 0, 0), "CANDIDATE", dm2.VoteResponse(1, True))
                no_op = engine._batch(1)
                engine._accept_response("n1", no_op, "LEADER", dm2.AppendResponse(1, True, 1))
            thread.start()
            try:
                self.assertTrue(waiting.wait(2))
                with engine.lock, patch("storage.os.replace", side_effect=OSError("fallo de commit")):
                    append = engine._batch(2)
                    with self.assertRaisesRegex(OSError, "fallo de commit"):
                        engine._accept_response("n1", append, "LEADER", dm2.AppendResponse(1, True, 2))
            finally:
                thread.join(3)
        self.assertEqual(responses, [None])
        restarted = self.make()
        self.assertEqual((restarted.commit_index, restarted.last_applied), (1, 1))
        self.assertEqual(restarted.log[1].command, b"mensaje")

    def test_waiting_client_cannot_confirm_replaced_bytes_with_same_request_id(self):
        engine = self.engine
        waiting = threading.Event()
        responses = []
        wait = engine._commit_cond.wait

        def wait_for_commit(timeout):
            waiting.set()
            return wait(timeout)

        def propose():
            responses.append(engine.client_command(header("c1", dm2.CLIENT_COMMAND, RID),
                                                   dm2.ClientCommand(b"original")))

        thread = threading.Thread(target=propose)
        with patch.object(engine, "_send"), patch.object(engine._commit_cond, "wait", side_effect=wait_for_commit), \
                contextlib.redirect_stdout(self.out):
            with engine.lock:
                engine._start_election()
                engine._accept_response("n1", dm2.RequestVote(1, 0, 0), "CANDIDATE", dm2.VoteResponse(1, True))
            thread.start()
            try:
                self.assertTrue(waiting.wait(2))
                changed = dm2.LogEntry.of_command(2, "c1", RID, b"reemplazo")
                response = self.append(2, 1, 1, 2, [changed])
                self.assertEqual(response, dm2.AppendResponse(2, True, 2))
            finally:
                thread.join(3)
        self.assertEqual(responses, [dm2.ClientResult(dm2.NOT_LEADER, 0, "n1")])

    def test_old_replies_do_not_move_replication_progress_but_higher_term_is_persisted(self):
        engine = self.engine
        with engine.lock, patch.object(engine, "_send"), contextlib.redirect_stdout(self.out):
            engine._start_election()
            engine._accept_response("n1", dm2.RequestVote(1, 0, 0), "CANDIDATE", dm2.VoteResponse(1, True))
            old = engine._batch(1)
            engine._step_down(2)
            engine._start_election()
            vote = dm2.RequestVote(3, 1, 1)
            engine._accept_response("n1", vote, "CANDIDATE", dm2.VoteResponse(3, True))
            current = engine._batch(2)
            engine._accept_response("n1", current, "LEADER", dm2.AppendResponse(3, True, 2))
            for reply in (dm2.AppendResponse(1, False, 0), dm2.AppendResponse(1, True, 1),
                          dm2.AppendResponse(3, False, 0)):
                engine._accept_response("n1", old, "LEADER", reply)
            engine._accept_response("n1", current, "LEADER", dm2.AppendResponse(2, False, 0))
            self.assertEqual((engine.next_index["n1"], engine.match_index["n1"]), (3, 2))
            engine._accept_response("n1", old, "LEADER", dm2.AppendResponse(4, False, 0))
            self.assertEqual((engine.state, engine.current_term, engine.voted_for), ("FOLLOWER", 4, None))
        self.assertEqual(self.make().current_term, 4)

    def test_delayed_rpc_reserves_peer_across_elections_and_replication(self):
        entered, release = threading.Event(), threading.Event()
        calls = []

        class BlockedClient:
            def call_rpc(self, endpoint, peer, sender, payload):
                calls.append(payload)
                entered.set()
                release.wait(3)
                reply = dm2.VoteResponse(payload.term, True)
                return dm2.Message(header(peer, dm2.VOTE_RESPONSE), reply)

        engine = self.make(peers={"n1": PEERS["n1"]})
        self.engine = engine
        engine.client = BlockedClient()
        with self.engine.lock, contextlib.redirect_stdout(self.out):
            engine._start_election()
        try:
            self.assertTrue(entered.wait(2))
            with engine.lock, contextlib.redirect_stdout(self.out):
                first = engine._in_flight["n1"]
                for _ in range(3):
                    engine._start_election()
                term = engine.current_term
                engine._accept_response("n1", dm2.RequestVote(term, 0, 0), "CANDIDATE",
                                        dm2.VoteResponse(term, True))
                for _ in range(10):
                    engine._replicate_to_all_peers()
                self.assertEqual(engine._in_flight, {"n1": first})
                self.assertEqual(len(calls), 1)
        finally:
            release.set()
        first.join(3)
        self.assertFalse(first.is_alive())
        self.assertEqual((engine.state, engine.current_term, engine.commit_index), ("LEADER", term, 0))

    def test_simulated_network_delay_does_not_hold_state_lock(self):
        sleeping, resume = threading.Event(), threading.Event()

        def delay(_seconds):
            sleeping.set()
            resume.wait(3)

        self.engine.set_fault_state(delay_ms=100)
        responses = []
        thread = threading.Thread(target=lambda: responses.append(self.vote("n1", 3)))
        with patch("raft_engine.time.sleep", side_effect=delay):
            thread.start()
            try:
                self.assertTrue(sleeping.wait(2))
                acquired = self.engine.lock.acquire(timeout=1)
                self.assertTrue(acquired)
                try:
                    self.engine.set_fault_state(isolated=True)
                finally:
                    self.engine.lock.release()
            finally:
                resume.set()
                thread.join(3)
        self.assertEqual(responses, [None])
        self.assertEqual(self.engine.current_term, 0)


class StorageTest(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(prefix="raft-st-"))
        self.addCleanup(shutil.rmtree, self.dir, True)

    def test_stale_temporary_is_removed(self):
        (self.dir / "raft_state-abc.tmp").write_text("{", encoding="utf-8")
        storage = NodeStorage(self.dir, "n2")
        storage.acquire()
        self.addCleanup(storage.close)
        self.assertEqual(list(self.dir.glob("*.tmp")), [])

    def test_corrupt_state_is_not_silently_reset(self):
        (self.dir / "raft_state.json").write_text("{no es json", encoding="utf-8")
        with self.assertRaises(StorageError):
            NodeStorage(self.dir, "n2").load()

    def test_other_node_state_is_rejected(self):
        engine = raft_engine.RaftEngine("n2", PEERS, self.dir, OfflineClient())
        self.addCleanup(engine.stop)
        with engine.lock, contextlib.redirect_stdout(io.StringIO()):
            engine._step_down(4)
        with self.assertRaises(StorageError):
            NodeStorage(self.dir, "n1").load()

    def test_exclusive_directory_blocks_second_engine_and_inspect_until_stop(self):
        first = raft_engine.RaftEngine("n2", PEERS, self.dir, OfflineClient())
        self.addCleanup(first.stop)
        with self.assertRaisesRegex(StorageError, "bloquear"):
            raft_engine.RaftEngine("n2", PEERS, self.dir, OfflineClient())
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(raft_engine.inspect(self.dir, "n2"), 1)
        self.assertIn("ERROR", out.getvalue())
        process = subprocess.run([sys.executable, str(ROOT / "nodes/python/node.py"), "--id", "n2",
                                  "--data-dir", str(self.dir), "--inspect"], text=True, capture_output=True, timeout=5)
        self.assertEqual(process.returncode, 1, process.stderr)
        self.assertIn("bloquear", process.stdout)
        first.stop()
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(raft_engine.inspect(self.dir, "n2"), 0)

    def test_out_of_range_term_is_rejected_and_exhaustion_does_not_wrap(self):
        storage = NodeStorage(self.dir, "n2")
        storage.acquire()
        try:
            storage.save(PersistentState(dm2.R64_MAX + 1, None))
            with self.assertRaises(StorageError):
                storage.load()
            storage.save(PersistentState(dm2.R64_MAX, "n1"))
        finally:
            storage.close()
        engine = raft_engine.RaftEngine("n2", PEERS, self.dir, OfflineClient())
        self.addCleanup(engine.stop)
        with engine.lock, contextlib.redirect_stderr(io.StringIO()) as out:
            engine._start_election()
        self.assertIn("término agotado", out.getvalue())
        self.assertEqual(storage.load().current_term, dm2.R64_MAX)

    def test_inspect_matches_java_format(self):
        storage = NodeStorage(self.dir, "n2")
        storage.acquire()
        try:
            storage.save(PersistentState(4, None, 1, [dm2.LogEntry.no_op(4),
                                                      dm2.LogEntry.of_command(4, "c1", RID, b"\x00")]))
        finally:
            storage.close()
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
