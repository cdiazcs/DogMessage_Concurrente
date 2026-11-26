import base64
import hashlib
from pathlib import Path
import sys
import unittest
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import e2ee_model
from chat_model import (ChatState, Operation, DEVICE, DEVICE_REVOKE, DIRECT, GROUP, REMOVE,
                        HISTORY_GRANT, LINK_START, LINK_APPROVE, LINK_CANCEL, SECURE_ENABLE, SECURE_SEND,
                        SEND)


def encoded(data):
    return base64.b64encode(data).decode("ascii")


def public(two):
    # Estructuras sintéticas para probar la proyección. OpenSSL prueba las curvas en el banco real.
    exchange = encoded(e2ee_model.SPKI + bytes([two]) * 64)
    signing = encoded(e2ee_model.SPKI + bytes([two + 1]) * 64)
    return e2ee_model.identity(exchange, signing), exchange, signing


def envelope(devices):
    wrapped = ";".join(":".join((d, encoded(bytes(12)), encoded(bytes(48)))) for d in sorted(devices))
    return ".".join(("E1", encoded(bytes(12)), encoded(bytes(17)), wrapped, encoded(bytes(64))))


class E2eeModelTest(unittest.TestCase):
    def setUp(self):
        self.key = bytes(range(32))
        self.state = ChatState(self.key)
        self.actors = [str(i) * 32 for i in range(1, 4)]
        self.tokens = [hashlib.sha256(bytes([i])).hexdigest() for i in range(3)]
        self.devices = []
        for index, (actor, token) in enumerate(zip(self.actors, self.tokens)):
            self.state.users[actor] = {"id": actor}
            self.state.sessions[token] = actor
            material = public(2 + index * 2)
            self.assertEqual(self.put(DEVICE, token, *material)["status"], "APPLIED")
            self.devices.append(material[0])

    def put(self, kind, *fields, request=None):
        request = request or uuid.uuid4()
        return self.state.apply(self.state.cursor + 1, Operation(kind, request.bytes, fields).encode(self.key))

    def direct(self):
        cid = self.put(DIRECT, self.tokens[0], self.actors[1])["conversation"]
        self.assertEqual(self.put(SECURE_ENABLE, self.tokens[0], cid)["status"], "APPLIED")
        return cid

    def message(self, cid, token, device, epoch, recipients):
        request = uuid.uuid4()
        return request, self.put(SECURE_SEND, token, cid, request.hex, device, epoch, envelope(recipients), request=request)

    def start_link(self):
        candidate = hashlib.sha256(b"candidate").hexdigest()
        self.state.sessions[candidate] = self.actors[0]
        new = public(12)
        link = self.put(LINK_START, candidate, *new, "a" * 64, "1700000000", "1700000120")["link"]
        return candidate, new[0], link

    def test_e2ee_blocks_plaintext_and_missing_wrap(self):
        cid = self.direct()
        self.assertEqual(self.put(SEND, self.tokens[0], cid, uuid.uuid4().hex, "claro")["reason"], "E2EE_REQUIRED")
        directory = self.state.secure_directory(self.tokens[0], cid)
        _, result = self.message(cid, self.tokens[0], self.devices[0], directory["epoch"], self.devices[:1])
        self.assertEqual(result["reason"], "E2EE_ROSTER_CHANGED")

    def test_epoch_change_is_ordered_and_snapshot_remains_opaque(self):
        group = uuid.uuid4()
        self.put(GROUP, self.tokens[0], group.hex, "Grupo", ",".join(self.actors[1:]), request=group)
        self.put(SECURE_ENABLE, self.tokens[0], group.hex)
        old = self.state.secure_directory(self.tokens[0], group.hex)["epoch"]
        self.put(REMOVE, self.tokens[0], group.hex, self.actors[2])
        _, rejected = self.message(group.hex, self.tokens[0], self.devices[0], old, self.devices)
        self.assertEqual(rejected["reason"], "E2EE_EPOCH_CHANGED")
        new = self.state.secure_directory(self.tokens[0], group.hex)["epoch"]
        mid, accepted = self.message(group.hex, self.tokens[0], self.devices[0], new, self.devices[:2])
        self.assertEqual(accepted["status"], "APPLIED")
        self.assertNotIn("text", self.state.messages[mid.hex])

    def test_first_identity_cannot_be_replaced(self):
        self.assertEqual(self.put(DEVICE, self.tokens[0], *public(20))["reason"], "DEVICE_LINK_REQUIRED")

    def test_qr_expiration_cancellation_and_one_use(self):
        candidate, device, link = self.start_link()
        self.assertEqual(self.put(LINK_APPROVE, self.tokens[0], link, self.devices[0], "1700000120")["reason"], "LINK_EXPIRED")
        self.assertEqual(self.put(LINK_APPROVE, candidate, link, self.devices[0], "1700000001")["reason"], "DEVICE_REQUIRED")
        self.assertEqual(self.put(LINK_CANCEL, self.tokens[1], link)["reason"], "UNKNOWN_LINK")
        self.assertEqual(self.put(LINK_CANCEL, candidate, link)["status"], "APPLIED")
        self.assertEqual(self.put(LINK_APPROVE, self.tokens[0], link, self.devices[0], "1700000002")["reason"], "LINK_USED")
        candidate, device, link = self.start_link()
        req = uuid.uuid4()
        result = self.put(LINK_APPROVE, self.tokens[0], link, self.devices[0], "1700000002", request=req)
        self.assertEqual(result["device"], device)
        self.assertEqual(self.put(LINK_APPROVE, self.tokens[0], link, self.devices[0], "1700000002", request=req), result)
        self.assertEqual(self.put(LINK_APPROVE, self.tokens[0], link, self.devices[0], "1700000002")["reason"], "LINK_USED")

    def test_history_grant_is_scoped_and_revocation_invalidates_session(self):
        cid = self.direct()
        directory = self.state.secure_directory(self.tokens[0], cid)
        mid, result = self.message(cid, self.tokens[0], self.devices[0], directory["epoch"], self.devices[:2])
        self.assertEqual(result["status"], "APPLIED")
        candidate, device, link = self.start_link()
        self.put(LINK_APPROVE, self.tokens[0], link, self.devices[0], "1700000003")
        body = envelope([device])
        self.assertEqual(self.put(HISTORY_GRANT, self.tokens[1], mid.hex, self.devices[1], device, body)["reason"],
                         "DEVICE_UNAVAILABLE")
        self.assertEqual(self.put(HISTORY_GRANT, self.tokens[0], mid.hex, self.devices[0], device, body)["status"], "APPLIED")
        page = self.state.query(candidate, "history", cid=cid)["messages"]
        self.assertEqual(page[0]["history_key"]["envelope"], body)
        self.assertNotIn("history_key", self.state.query(self.tokens[1], "history", cid=cid)["messages"][0])
        self.assertEqual(len(page), 1)
        self.put(DEVICE_REVOKE, self.tokens[0], device)
        with self.assertRaisesRegex(ValueError, "AUTH_REQUIRED"):
            self.state.query(candidate, "history", cid=cid)

    def test_e1_requires_canonical_no_duplicate_wraps(self):
        sample = envelope(self.devices[:2])
        sections = sample.split(".")
        parts = sections[3].split(";")
        sections[3] = ";".join(reversed(parts))
        with self.assertRaisesRegex(ValueError, "INVALID_E2EE_ROSTER"):
            e2ee_model.envelope(".".join(sections))
        sections[3] = ";".join([parts[0], parts[0]])
        with self.assertRaisesRegex(ValueError, "INVALID_E2EE_ENVELOPE"):
            e2ee_model.envelope(".".join(sections))


if __name__ == "__main__":
    unittest.main()
