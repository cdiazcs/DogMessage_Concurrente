"""Pruebas unitarias de framing para el protocolo DM1."""
import importlib.util
from pathlib import Path
import socket
import struct
import unittest

NODE_PATH = Path(__file__).resolve().parents[1] / "nodes/python/node.py"
spec = importlib.util.spec_from_file_location("dog_node", NODE_PATH)
node = importlib.util.module_from_spec(spec)
spec.loader.exec_module(node)


class ProtocolTest(unittest.TestCase):
    def test_ping_framing(self):
        body = "DM1|PING|n2|" + "a" * 32
        data = node.encode_frame(body)
        self.assertEqual(struct.unpack("!I", data[:4])[0], len(body.encode("utf-8")))
        self.assertEqual(data[4:].decode(), body)

    def test_invalid_length(self):
        for body in ["", "a" * (node.MAX_FRAME + 1)]:
            with self.subTest(length=len(body)):
                with self.assertRaises(ValueError):
                    node.encode_frame(body)

    def test_read_fragmented_message(self):
        left, right = socket.socketpair()
        try:
            data = node.encode_frame("DM1|PING|n2|" + "a" * 32)
            for part in [data[:1], data[1:3], data[3:7], data[7:]]:
                left.sendall(part)
            self.assertEqual(node.recv_frame(right), "DM1|PING|n2|" + "a" * 32)
        finally:
            left.close()
            right.close()

    def test_reject_oversized_received_length(self):
        left, right = socket.socketpair()
        try:
            left.sendall(struct.pack("!I", node.MAX_FRAME + 1))
            with self.assertRaises(ValueError):
                node.recv_frame(right)
        finally:
            left.close()
            right.close()


if __name__ == "__main__":
    unittest.main()
