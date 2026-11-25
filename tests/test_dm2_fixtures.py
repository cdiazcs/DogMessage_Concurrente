"""Vectores DM2 compartidos (protocol/fixtures/dm2.json) contra el codec Python."""
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "nodes" / "python"))
import dm2_codec as dm2  # noqa: E402

FIXTURES = json.loads((ROOT / "protocol" / "fixtures" / "dm2.json").read_text(encoding="utf-8"))
SERVER_ID = "n2"
MEMBERS = {"n1", "n2", "n3"}
REQUEST_ID = bytes.fromhex("0123456789abcdef0123456789abcdef")


def fields(payload) -> dict:
    """Traduce un payload a los nombres de campo usados en dm2.json."""
    if isinstance(payload, dm2.RequestVote):
        return {"term": payload.term, "lastLogIndex": payload.last_log_index, "lastLogTerm": payload.last_log_term}
    if isinstance(payload, dm2.VoteResponse):
        return {"term": payload.term, "voteGranted": payload.vote_granted}
    if isinstance(payload, dm2.AppendEntries):
        out = {"term": payload.term, "prevLogIndex": payload.prev_log_index, "prevLogTerm": payload.prev_log_term,
               "leaderCommit": payload.leader_commit, "entryCount": len(payload.entries),
               "entryIndexes": [payload.prev_log_index + i + 1 for i in range(len(payload.entries))],
               "entryTerms": [e.term for e in payload.entries], "entryKinds": [e.kind for e in payload.entries]}
        commands = [e for e in payload.entries if e.kind == dm2.COMMAND]
        if commands:
            out.update(clientId=commands[0].client_id, clientRequestId=commands[0].client_request_id.hex(),
                       commandHex=commands[0].command.hex())
        return out
    if isinstance(payload, dm2.AppendResponse):
        return {"term": payload.term, "success": payload.success, "matchIndex": payload.match_index}
    if isinstance(payload, dm2.ClientCommand):
        return {"commandHex": payload.command.hex()}
    if isinstance(payload, dm2.ClientResult):
        return {"status": payload.status, "logIndex": payload.log_index, "leaderHint": payload.leader_hint}
    if isinstance(payload, dm2.ErrorMsg):
        return {"code": payload.code}
    raise AssertionError(f"payload inesperado {payload!r}")


def server_action(data: bytes):
    """Lo que haría el servidor con los bytes recibidos (fin de flujo incluido)."""
    try:
        body = dm2.parse_frame(data)
    except dm2.FrameError:
        return ("close", None)
    decision = dm2.classify_request(body, MEMBERS)
    if isinstance(decision, dm2.Close):
        return ("close", None)
    if isinstance(decision, dm2.Reject):
        response = dm2.frame(dm2.reply(decision.header, dm2.ErrorMsg(decision.code), SERVER_ID))
        return ("error", response)
    return ("decode", decision.message)


def pending_request(response_type: int) -> dm2.Message:
    """Intento activo con el que el cliente correlaciona las respuestas de los vectores."""
    if response_type == dm2.VOTE_RESPONSE:
        payload = dm2.RequestVote(3, 0, 0)
    elif response_type == dm2.APPEND_RESPONSE:
        payload = dm2.AppendEntries(4, 7, 3, 9, (dm2.LogEntry.no_op(4), dm2.LogEntry.no_op(4)))
    else:
        payload = dm2.ClientCommand(b"\x00")
    return dm2.Message(dm2.Header(dm2.RAFT, payload.TYPE, REQUEST_ID, "n1"), payload)


class Dm2FixturesTest(unittest.TestCase):
    def test_fixture_count(self):
        self.assertEqual(len(FIXTURES), 32)

    def test_all_fixtures(self):
        passed = []
        for case in FIXTURES:
            with self.subTest(case=case["name"]):
                self.run_case(case)
                passed.append(case["name"])
        print(f"\n  fixtures DM2 en verde: {len(passed)}/{len(FIXTURES)}", file=sys.stderr)
        self.assertEqual(len(passed), len(FIXTURES))

    def run_case(self, case):
        data = bytes.fromhex(case["hex"])
        expected = case["expected"]
        action = expected["action"]
        if case["direction"] == "server":
            got, value = server_action(data)
            self.assertEqual(got, action)
            if action == "decode":
                self.check_decoded(value, expected, data)
            elif action == "error":
                self.assertEqual(value.hex(), expected["responseHex"])
                self.assertEqual(dm2.decode(value[4:]).payload.code, expected["code"])
            return
        # Dirección cliente: respuesta recibida para un intento activo.
        body = dm2.parse_frame(data)
        response_type = body[5]
        if action == "decode":
            message = dm2.decode(body)
            self.check_decoded(message, expected, data)
            if response_type != dm2.ERROR:
                dm2.check_response(pending_request(response_type), SERVER_ID, body)
        elif action == "discard":
            with self.assertRaises(dm2.ResponseError):
                dm2.check_response(pending_request(response_type), SERVER_ID, body)
        else:
            self.fail(f"acción desconocida {action}")

    def check_decoded(self, message, expected, original):
        got = fields(message.payload)
        for key, value in expected.items():
            if key != "action":
                self.assertEqual(got[key], value, key)
        self.assertEqual(dm2.encode_frame(message), original, "re-codificación byte a byte")


class Dm2CodecRulesTest(unittest.TestCase):
    """Reglas del contrato que los vectores no cubren directamente."""

    def test_encoder_rejects_invalid_messages(self):
        invalid = [
            lambda: dm2.Header(1, 1, bytes(16), "n1"),
            lambda: dm2.Header(1, 1, REQUEST_ID, "n|"),
            lambda: dm2.Header(1, 1, REQUEST_ID, "x" * 33),
            lambda: dm2.RequestVote(0, 0, 0),
            lambda: dm2.RequestVote(1 << 63, 0, 0),
            lambda: dm2.RequestVote(3, 1, 0),
            lambda: dm2.RequestVote(3, 1, 4),
            lambda: dm2.VoteResponse(0, True),
            lambda: dm2.AppendResponse(4, False, 9),
            lambda: dm2.AppendEntries(4, 0, 0, 0, (dm2.LogEntry.no_op(5),)),
            lambda: dm2.AppendEntries(4, 3, 3, 0, (dm2.LogEntry.no_op(2),)),
            lambda: dm2.AppendEntries(4, 0, 0, 0, tuple(dm2.LogEntry.no_op(4) for _ in range(257))),
            lambda: dm2.AppendEntries(4, dm2.R64_MAX, 3, 0, (dm2.LogEntry.no_op(4),)),
            lambda: dm2.LogEntry(4, 2),
            lambda: dm2.LogEntry.of_command(4, "c1", bytes(16), b"x"),
            lambda: dm2.ClientCommand(b""),
            lambda: dm2.ClientCommand(b"x" * 65_537),
            lambda: dm2.ClientResult(0, 0),
            lambda: dm2.ClientResult(2, 0, "n1"),
            lambda: dm2.ErrorMsg(5),
        ]
        for i, build in enumerate(invalid):
            with self.subTest(i=i), self.assertRaises(ValueError):
                build()

    def test_limits_round_trip(self):
        entries = (dm2.LogEntry.of_command(dm2.R64_MAX, "c" * 32, b"\xff" * 16, b"\x00" * 65_536),)
        append = dm2.AppendEntries(dm2.R64_MAX, dm2.R64_MAX - 1, 1, dm2.R64_MAX, entries)
        message = dm2.Message(dm2.Header(dm2.RAFT, dm2.APPEND_ENTRIES, REQUEST_ID, "n1"), append)
        self.assertEqual(dm2.decode(dm2.encode(message)), message)
        self.assertEqual(len(dm2.encode(message)), dm2.HEADER_BYTES + 2 + dm2.APPEND_FIXED_BYTES
                         + dm2.entry_size(entries[0]))

    def test_dm1_is_not_dm2(self):
        decision = dm2.classify_request(b"DM1|PING|n1|" + b"a" * 32, MEMBERS)
        self.assertIsInstance(decision, dm2.Close)

    def test_client_command_from_non_member_is_accepted(self):
        message = dm2.Message(dm2.Header(dm2.RAFT, dm2.CLIENT_COMMAND, REQUEST_ID, "c9"), dm2.ClientCommand(b"x"))
        self.assertIsInstance(dm2.classify_request(dm2.encode(message), MEMBERS), dm2.Accept)

    def test_response_from_unexpected_peer_is_discarded(self):
        request = pending_request(dm2.VOTE_RESPONSE)
        response = dm2.reply(request.header, dm2.VoteResponse(3, True), "n3")
        with self.assertRaises(dm2.ResponseError):
            dm2.check_response(request, "n2", response)

    def test_stale_term_and_wrong_match_index_are_discarded(self):
        vote = pending_request(dm2.VOTE_RESPONSE)
        append = pending_request(dm2.APPEND_RESPONSE)
        cases = [(vote, dm2.VoteResponse(2, False)), (append, dm2.AppendResponse(3, False, 0)),
                 (append, dm2.AppendResponse(4, True, 8))]
        for request, payload in cases:
            with self.subTest(payload=payload), self.assertRaises(dm2.ResponseError):
                dm2.check_response(request, "n2", dm2.reply(request.header, payload, "n2"))
        ok = dm2.check_response(append, "n2", dm2.reply(append.header, dm2.AppendResponse(4, True, 9), "n2"))
        self.assertTrue(ok.payload.success)


if __name__ == "__main__":
    unittest.main()
