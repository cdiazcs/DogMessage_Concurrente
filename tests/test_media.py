import hashlib
import os
from pathlib import Path
import struct
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tools"), str(ROOT / "nodes/python")]
from media_store import BLOCK, GET, MAX_FILE, PUT, SEAL, STATUS, BlobStore, admits_attachment, pack, proof, remote, unpack
from chat_model import ATTACH, ChatState, Operation, decode, valid_filename
from gateway.media_gateway import detected_type


class MediaStorageTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.store = BlobStore(self.root)
        self.data = b"\x83" * BLOCK + b"final"
        self.sha = hashlib.sha256(self.data).hexdigest()

    def apply(self, op, offset=0, body=b"", size=None):
        return self.store.apply(op, self.sha, size if size is not None else len(self.data), offset, body)

    def test_empty_large_and_bad_offsets(self):
        for size in (0, MAX_FILE + 1):
            with self.assertRaisesRegex(ValueError, "INVALID_BLOCK"):
                self.apply(STATUS, size=size)
        for offset in (1, BLOCK * 2, -1):
            with self.assertRaisesRegex(ValueError, "INVALID_BLOCK"):
                self.apply(PUT, offset, b"x")
        with self.assertRaisesRegex(ValueError, "OUT_OF_ORDER"):
            self.apply(PUT, BLOCK, b"final")

    def test_reopen_partial_and_duplicate(self):
        first = self.data[:BLOCK]
        self.assertEqual(self.apply(PUT, 0, first), self.apply(PUT, 0, first))
        with self.assertRaisesRegex(ValueError, "BLOCK_CONFLICT"):
            self.apply(PUT, 0, b"=" * BLOCK)
        with self.assertRaisesRegex(ValueError, "INCOMPLETE_BLOB"):
            self.apply(SEAL)
        with self.assertRaisesRegex(ValueError, "MISSING_BLOB"):
            self.apply(GET)
        self.store = BlobStore(self.root)
        self.assertEqual(struct.unpack(">BQ", self.apply(STATUS)), (0, BLOCK))
        self.apply(PUT, BLOCK, b"final")
        self.assertEqual(struct.unpack(">BQ", self.apply(SEAL)), (1, len(self.data)))
        self.assertEqual(self.apply(GET) + self.apply(GET, BLOCK), self.data)

    def test_interrupted_partial_tail_is_truncated(self):
        partial, _ = self.store.paths(self.sha)
        partial.write_bytes(self.data[:BLOCK + 2])
        self.assertEqual(struct.unpack(">BQ", self.apply(STATUS)), (0, BLOCK))
        self.assertEqual(partial.stat().st_size, BLOCK)

    def test_bad_digest_and_repair_corrupt_final(self):
        self.apply(PUT, 0, b"=" * BLOCK)
        self.apply(PUT, BLOCK, b"final")
        with self.assertRaisesRegex(ValueError, "HASH_MISMATCH"):
            self.apply(SEAL)
        part, final = self.store.paths(self.sha)
        self.assertFalse(part.exists())
        final.write_bytes(b"=" * len(self.data))
        self.assertEqual(struct.unpack(">BQ", self.apply(STATUS)), (0, 0))
        self.apply(PUT, 0, self.data[:BLOCK])
        self.apply(PUT, BLOCK, b"final")
        self.apply(SEAL)
        self.assertEqual(final.read_bytes(), self.data)

    def test_fsync_failure_does_not_acknowledge_block(self):
        with patch("media_store.os.fsync", side_effect=OSError("disco de prueba")):
            with self.assertRaises(OSError):
                self.apply(PUT, 0, self.data[:BLOCK])
        self.assertEqual(struct.unpack(">BQ", self.apply(STATUS)), (0, 0))

    def test_filename_and_detected_type(self):
        for name in ("../x", "x/y", "x\\y", "C:drive", "a\x00", ".."):
            self.assertFalse(valid_filename(name))
        self.assertTrue(valid_filename("foto de prueba.png"))
        file = self.root / "test.png"
        file.write_bytes(b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR" + struct.pack(">II", 1, 1))
        self.assertEqual(detected_type(file), "image/png")
        file.write_bytes(b"<svg onload=alert(1)>")
        self.assertEqual(detected_type(file), "application/octet-stream")

    def test_dmb1_fixture(self):
        for line in (ROOT / "protocol/fixtures/media.tsv").read_text().splitlines():
            if line.startswith("#"):
                continue
            label, key, sha, offset, payload, wire = line.split("\t")
            packet = bytes.fromhex(wire)
            secret = bytes.fromhex(key)
            data = bytes.fromhex(payload)
            kind = STATUS if label == "status" else PUT
            request = bytes(range(1, 17))
            self.assertEqual(pack(secret, kind, request, "n2", sha, 8195, int(offset), data), packet)
            self.assertEqual(unpack(secret, packet[4:]), (kind, request, "n2", sha, 8195, int(offset), data))
            with self.assertRaisesRegex(ValueError, "INVALID_MAC"):
                unpack(bytes(32), packet[4:])

    def test_binary_success_reply_is_not_decoded_as_ascii(self):
        key = bytes(range(32))
        request = bytes(range(1, 17))
        response = b"\0\xff\x80"
        raw = pack(key, GET | 128, request, "n2", self.sha, 2, 0, response)[4:]
        with patch("media_store.socket.create_connection"), patch("media_store.frame", return_value=raw), \
                patch("media_store.uuid.uuid4") as random:
            random.return_value.bytes = request
            self.assertEqual(remote(("127.0.0.1", 1234), key, "n2", GET, self.sha, 2), b"\xff\x80")

    def test_2pc_requires_each_authentic_receipt(self):
        chat_key = bytes(range(32))
        receipt_key = bytes(reversed(range(32)))
        members = {"n1", "n2", "n3"}
        cases = {node: proof(receipt_key, node, self.sha, len(self.data)).hex() for node in members}
        request = bytes(range(1, 17))
        fields = ("7" * 64, "a" * 32, request.hex(), "FILE", "x.zip", "application/zip",
                  str(len(self.data)), self.sha)
        with patch.dict(os.environ, {"DOG_CHAT_KEY": chat_key.hex(), "DOG_MEDIA_RECEIPT_KEY": receipt_key.hex()}):
            for rows, valid in ((list(cases.items()), True), (list(cases.items())[:2], False),
                                ([("n1", "0" * 64), *[(n, cases[n]) for n in ("n2", "n3")]], False)):
                raw = Operation(ATTACH, request, (*fields, ",".join(f"{node}:{mac}" for node, mac in rows))).encode(chat_key)
                self.assertEqual(admits_attachment(raw, members, request, "chat-app"), valid)

    def test_signed_metadata_is_small_and_ordered(self):
        secret = bytes(range(32))
        actor = "4" * 32
        session = "9" * 64
        conversation = "1" * 32
        state = ChatState(secret)
        state.sessions[session] = actor
        state.conversations[conversation] = {"members": [actor], "messages": []}
        request = bytes(range(1, 17))
        operation = Operation(ATTACH, request, (session, conversation, request.hex(), "FILE", "x.zip",
                                                 "application/zip", "8197", self.sha, "n1:" + "a" * 64 + ",n2:" + "b" * 64))
        raw = operation.encode(secret)
        self.assertLess(len(raw), 512)
        self.assertEqual(decode(raw, secret), operation)
        result = state.apply(42, raw)
        self.assertEqual(result["status"], "APPLIED")
        self.assertEqual(state.apply(43, raw), result)
        self.assertEqual(state.messages[request.hex()]["position"], 42)


if __name__ == "__main__":
    unittest.main()
