import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from gateway.chat_gateway import Gateway, encode_reply, wire_positions

REQUEST_ID = "0123456789abcdef0123456789abcdef"


class GatewayValidationTest(unittest.TestCase):
    def setUp(self):
        self.gateway = Gateway(SimpleNamespace(consensus=SimpleNamespace(coordinator=None)), bytes(32))

    def request(self, op, data=None, **extra):
        request = {"v": 1, "id": REQUEST_ID, "op": op, "data": data or {}, **extra}
        return self.gateway.dispatch(json.dumps(request).encode())

    def test_rejects_duplicate_keys_and_invalid_utf8(self):
        self.assertEqual(self.gateway.dispatch(b'{"v":1,"v":1}')["error"], "INVALID_JSON")
        self.assertEqual(self.gateway.dispatch(b"\xff")["error"], "INVALID_REQUEST")

    def test_rejects_unsupported_versions_and_non_uuid(self):
        self.assertEqual(self.request("users", v=True)["error"], "INVALID_VERSION")
        self.assertEqual(self.request("users", id="0" * 32)["error"], "INVALID_ID")
        self.assertEqual(self.request("users", id="12")["error"], "INVALID_ID")

    def test_requires_session_and_rejects_impersonation_fields(self):
        self.assertEqual(self.request("users")["error"], "AUTH_REQUIRED")
        self.assertEqual(self.request("send", {"senderId": REQUEST_ID, "conversation": REQUEST_ID,
                                              "text": "mensaje"})["error"], "INVALID_DATA")
        self.assertEqual(self.request("users", senderId=REQUEST_ID)["error"], "INVALID_REQUEST")

    def test_enforces_size_and_cursor_bounds(self):
        self.assertEqual(self.request("send", {"conversation": REQUEST_ID, "text": "🐶" * 1025})
                         ["error"], "INVALID_DATA")
        self.assertEqual(self.request("users", min_cursor=str(1 << 63))["error"], "INVALID_CURSOR")
        self.assertEqual(self.request("users", min_cursor=0)["error"], "INVALID_CURSOR")
        self.assertEqual(self.request("history", {"after": "0", "conversation": REQUEST_ID, "limit": True})
                         ["error"], "INVALID_LIMIT")

    def test_positions_survive_javascript_integer_limits(self):
        sample = {"messages": [{"position": (1 << 63) - 1}], "more": False, "next_cursor": 1 << 62}
        observed = json.loads(encode_reply(wire_positions(sample)))
        self.assertEqual(observed["messages"][0]["position"], str((1 << 63) - 1))
        self.assertEqual(observed["next_cursor"], str(1 << 62))
        self.assertFalse(observed["more"])


if __name__ == "__main__":
    unittest.main()
