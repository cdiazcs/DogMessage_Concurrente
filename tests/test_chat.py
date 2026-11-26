from pathlib import Path
import sys
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
import chat_model as m


class ChatModelTest(unittest.TestCase):
    def setUp(self):
        self.key = bytes(range(32))
        self.state = m.ChatState(self.key)
        self.position = 0
        self.events = []
        self.credentials = m.new_credential("prueba-únicamente")

    def operation(self, kind, *fields, token=None):
        return m.Operation(kind, token or uuid.uuid4().bytes, tuple(fields))

    def apply(self, op, step=1):
        self.position += step
        raw = op.encode(self.key)
        self.events.append((self.position, raw))
        return self.state.apply(self.position, raw)

    def commit(self, kind, *fields):
        return self.apply(self.operation(kind, *fields))

    def setup_users(self, count):
        users, sessions = [], []
        for i in range(count):
            user = m.identity()
            self.assertEqual(self.commit(m.REGISTER, user, f"user{i}", f"Usuario {i}", *self.credentials)["status"],
                             "APPLIED")
            session = m.new_session()[1]
            self.assertEqual(self.commit(m.SESSION, user, session)["status"], "APPLIED")
            users.append(user)
            sessions.append(session)
        return users, sessions

    def test_password_session_and_public_directory(self):
        users, sessions = self.setup_users(2)
        self.assertEqual(self.state.authenticate("user0", "prueba-únicamente"), users[0])
        with self.assertRaisesRegex(m.Rejected, "INVALID_CREDENTIALS"):
            self.state.authenticate("user0", "contraseña errónea")
        user = self.state.query(sessions[0], "users")[0]
        self.assertEqual(set(user), {"id", "display", "username", "state"})
        self.assertEqual(self.commit(m.REGISTER, m.identity(), "user0", "Duplicado", *self.credentials)["reason"],
                         "USER_EXISTS")
        self.assertEqual(self.commit(m.REVOKE, sessions[1])["status"], "APPLIED")
        with self.assertRaisesRegex(m.Rejected, "AUTH_REQUIRED"):
            self.state.query(sessions[1], "users")

    def test_direct_membership_order_and_status(self):
        users, sessions = self.setup_users(3)
        cid = self.commit(m.DIRECT, sessions[0], users[1])["conversation"]
        self.assertEqual(self.commit(m.DIRECT, sessions[1], users[0])["conversation"], cid)
        with self.assertRaisesRegex(m.Rejected, "FORBIDDEN"):
            self.state.query(sessions[2], "history", cid)
        op = self.operation(m.SEND, sessions[0], cid, m.identity(), "texto 🐶")
        self.assertEqual(self.state.query(sessions[0], "status", request=op.request.hex())["status"], "UNKNOWN")
        result = self.apply(op, step=4)
        self.assertEqual(self.state.query(sessions[0], "status", request=op.request.hex()), result)
        self.assertEqual(self.state.query(sessions[1], "history", cid)["messages"][0]["sender"], users[0])
        self.assertEqual(self.state.query(sessions[1], "history", cid)["messages"][0]["position"], result["position"])
        with self.assertRaisesRegex(m.Rejected, "FORBIDDEN"):
            self.state.query(sessions[2], "status", request=op.request.hex())

    def test_dedup_conflict_and_invalid_message(self):
        users, sessions = self.setup_users(2)
        cid = self.commit(m.DIRECT, sessions[0], users[1])["conversation"]
        op = self.operation(m.SEND, sessions[0], cid, m.identity(), "uno")
        result = self.apply(op)
        self.assertEqual(self.apply(op), result)
        conflict = self.operation(m.SEND, *op.fields[:-1], "dos", token=op.request)
        self.assertEqual(self.apply(conflict)["reason"], "ID_CONFLICT")
        for text in ("", "🐶" * 1025):
            self.assertEqual(self.commit(m.SEND, sessions[0], cid, m.identity(), text)["reason"], "INVALID_TEXT")
        self.assertEqual(len(self.state.messages), 1)

    def test_group_owner_membership_and_reconstruction(self):
        users, sessions = self.setup_users(3)
        cid = m.identity()
        self.assertEqual(self.commit(m.GROUP, sessions[0], cid, "Equipo", ",".join(users))["status"], "APPLIED")
        self.assertEqual(self.commit(m.REMOVE, sessions[1], cid, users[2])["reason"], "ADMIN_REQUIRED")
        self.commit(m.SEND, sessions[2], cid, m.identity(), "antes")
        self.commit(m.REMOVE, sessions[0], cid, users[2])
        self.assertEqual(self.commit(m.SEND, sessions[2], cid, m.identity(), "después")["reason"], "FORBIDDEN")
        with self.assertRaisesRegex(m.Rejected, "FORBIDDEN"):
            self.state.query(sessions[2], "history", cid)
        self.commit(m.ADD, sessions[0], cid, users[2])
        self.assertEqual(len(self.state.query(sessions[2], "history", cid)["messages"]), 1)
        self.assertEqual(set(self.state.query(sessions[0], "members", cid)), set(users))
        recovered = m.ChatState(self.key)
        for index, raw in self.events:
            recovered.apply(index, raw)
        self.assertEqual(recovered.public_digest(), self.state.public_digest())

    def test_cursor_skips_internal_positions(self):
        users, sessions = self.setup_users(2)
        cid = self.commit(m.DIRECT, sessions[0], users[1])["conversation"]
        for i in range(3):
            self.apply(self.operation(m.SEND, sessions[0], cid, m.identity(), str(i)), step=4)
        first = self.state.query(sessions[0], "sync", cid, limit=2)
        second = self.state.query(sessions[0], "history", cid, after=first["next_cursor"], limit=2)
        self.assertTrue(first["more"])
        self.assertFalse(second["more"])
        self.assertEqual([msg["text"] for msg in first["messages"] + second["messages"]], ["0", "1", "2"])
        self.assertEqual(second["next_cursor"], self.state.cursor)

    def test_signature_required_and_secrets_not_in_public_digest_or_replies(self):
        salt, _, digest = self.credentials
        user = m.identity()
        op = self.operation(m.REGISTER, user, "user0", "Usuario", *self.credentials)
        self.assertIsNone(self.state.apply(1, op.encode(bytes(32))))
        self.assertEqual(self.state.users, {})
        self.position = 1
        result = self.apply(op)
        self.assertNotIn(salt, str(result))
        self.assertNotIn(digest, str(result))
        self.assertNotIn("prueba-únicamente".encode(), op.encode(self.key))

    def test_cross_language_codec_and_pbkdf2_vectors(self):
        for line in (ROOT / "protocol/fixtures/chat.tsv").read_text().splitlines():
            name, valid, encoded = line.split("\t")
            with self.subTest(name=name):
                raw = bytes.fromhex(encoded)
                if valid == "1":
                    self.assertEqual(m.decode(raw, bytes(32)).encode(bytes(32)), raw)
                else:
                    with self.assertRaises(m.Rejected):
                        m.decode(raw, bytes(32))
        salt, password, expected = (ROOT / "protocol/fixtures/chat_pbkdf2.tsv").read_text().strip().split("\t")
        self.assertEqual(m.verifier(bytes.fromhex(password).decode("utf-8"), bytes.fromhex(salt)), expected)
