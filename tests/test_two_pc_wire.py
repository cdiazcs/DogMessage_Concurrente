from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "nodes/python"))
import dm2_codec as dm
import two_pc_wire as wire


class TwoPcWireTest(unittest.TestCase):
    def test_fixtures(self):
        for line in (ROOT / "protocol/fixtures/two_pc.tsv").read_text().splitlines():
            label, valid, data = line.split("\t")
            with self.subTest(label=label):
                body = dm.parse_frame(bytes.fromhex(data))
                if valid == "0":
                    with self.assertRaises(ValueError):
                        algorithm, kind, request_id, sender, reader = wire.header(body)
                        wire.read_payload(kind, reader)
                    continue
                algorithm, kind, request_id, sender, reader = wire.header(body)
                self.assertEqual(algorithm, 2)
                value = wire.read_payload(kind, reader)
                if isinstance(value, wire.Tx):
                    payload = value.encode()
                elif kind == wire.SUBMIT:
                    payload = wire.command_bytes(value)
                elif kind == 0x81:
                    payload = bytes([value[0]]) + wire.position(value[1], True) + wire.node_bytes(value[2])
                elif kind == 0xff:
                    payload = bytes([value])
                else:
                    payload = wire.position(value[0]) + value[1] + (bytes([value[2]]) if len(value) == 3 else b"")
                self.assertEqual(dm.frame(wire.encode(kind, request_id, sender, payload)), bytes.fromhex(data))
