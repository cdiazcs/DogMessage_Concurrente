"""Transporte TCP del nodo Python (node.py) y prueba cruzada DM2 con NodeMain Java."""
import queue
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "nodes" / "python"))
sys.path.insert(0, str(ROOT / "tools"))
import dm2_codec as dm2  # noqa: E402
import node  # noqa: E402

LOCAL = "127.0.0.1"
MEMBERS = {"n1", "n2", "n3"}


class VoteStub(node.RpcHandler):
    """Réplica mínima en memoria: solo para ejercitar el transporte, no es el motor Raft."""

    def __init__(self):
        self.lock = threading.Lock()
        self.term = 0
        self.voted_for = None
        self.seen = []

    def request_vote(self, header, request):
        with self.lock:
            self.seen.append(("vote", header.sender_id, request.term))
            if request.term > self.term:
                self.term, self.voted_for = request.term, None
            granted = request.term == self.term and self.voted_for in (None, header.sender_id)
            if granted:
                self.voted_for = header.sender_id
            return dm2.VoteResponse(self.term, granted)

    def append_entries(self, header, request):
        with self.lock:
            self.seen.append(("append", header.sender_id, request.term))
            self.term = max(self.term, request.term)
            return dm2.AppendResponse(self.term, True, request.last_index)

    def client_command(self, header, request):
        return dm2.ClientResult(dm2.NOT_LEADER, 0, "n1")


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind((LOCAL, 0))
        return probe.getsockname()[1]


def vote_message(sender="n1", term=3, request_id=None) -> dm2.Message:
    payload = dm2.RequestVote(term, 0, 0)
    return dm2.Message(dm2.Header(dm2.RAFT, dm2.REQUEST_VOTE, request_id or node.new_request_id(), sender), payload)


class ServerCase(unittest.TestCase):
    timeout = 1.0
    max_connections = 4

    def setUp(self):
        self.handler = VoteStub()
        dispatcher = node.Dispatcher("n2", MEMBERS, self.handler)
        self.server = node.TcpServer(LOCAL, 0, dispatcher, self.max_connections, self.timeout)
        self.server.start()
        self.endpoint = (LOCAL, self.server.port)
        self.client = node.TcpClient(timeout=self.timeout)

    def tearDown(self):
        self.server.close()

    def raw(self, data: bytes, pause: float = 0.0) -> bytes:
        """Envía bytes crudos (opcionalmente de a uno) y devuelve todo lo recibido hasta el cierre."""
        with socket.create_connection(self.endpoint, timeout=5) as conn:
            if pause:
                for byte in data:
                    conn.sendall(bytes([byte]))
                    time.sleep(pause)
            else:
                conn.sendall(data)
            chunks = []
            while True:
                part = conn.recv(4096)
                if not part:
                    return b"".join(chunks)
                chunks.append(part)


class FramingTest(unittest.TestCase):
    def test_recv_exact_byte_by_byte(self):
        body = dm2.encode(vote_message())
        left, right = socket.socketpair()
        try:
            def drip():
                for byte in dm2.frame(body):
                    left.sendall(bytes([byte]))
                    time.sleep(0.001)
            threading.Thread(target=drip, daemon=True).start()
            self.assertEqual(node.read_frame(right, timeout=5), body)
        finally:
            left.close()
            right.close()

    def test_grouped_frames_only_first_is_read(self):
        first, second = dm2.encode(vote_message()), dm2.encode(vote_message(term=9))
        left, right = socket.socketpair()
        try:
            left.sendall(dm2.frame(first) + dm2.frame(second))
            self.assertEqual(node.read_frame(right, timeout=1), first)
        finally:
            left.close()
            right.close()

    def test_budget_is_per_frame_not_per_recv(self):
        """Un emisor que gotea un byte cada 0,2 s no extiende el plazo total."""
        left, right = socket.socketpair()
        stop = threading.Event()
        try:
            def drip():
                for byte in dm2.frame(dm2.encode(vote_message())):
                    if stop.wait(0.2):
                        return
                    try:
                        left.sendall(bytes([byte]))
                    except OSError:
                        return
            threading.Thread(target=drip, daemon=True).start()
            start = time.monotonic()
            with self.assertRaises(socket.timeout):
                node.read_frame(right, timeout=1.0)
            self.assertLess(time.monotonic() - start, 1.5)
        finally:
            stop.set()
            left.close()
            right.close()

    def test_invalid_prefix_rejected_before_body(self):
        for prefix in (struct.pack(">I", 0), struct.pack(">I", dm2.MAX_BODY + 1)):
            left, right = socket.socketpair()
            try:
                left.sendall(prefix)
                with self.assertRaises(dm2.FrameError):
                    node.read_frame(right, timeout=1)
            finally:
                left.close()
                right.close()


class ServerTest(ServerCase):
    def test_dm1_ping(self):
        self.assertEqual(self.client.ping(self.endpoint, "n1"), "n2")
        answer = self.raw(node.encode_frame("DM1|PING|n1|corto"))
        self.assertEqual(answer[4:], b"DM1|ERROR|n2|" + b"0" * 32)

    def test_dm2_request_fragmented_byte_by_byte(self):
        request = vote_message(term=5)
        answer = self.raw(dm2.encode_frame(request), pause=0.001)
        response = dm2.check_response(request, "n2", dm2.parse_frame(answer))
        self.assertEqual(response.payload, dm2.VoteResponse(5, True))

    def test_rpc_client_round_trip(self):
        response = self.client.call_rpc(self.endpoint, "n2", "n1", dm2.RequestVote(7, 0, 0))
        self.assertEqual(response.payload, dm2.VoteResponse(7, True))
        self.assertEqual(response.header.sender_id, "n2")

    def test_correlated_errors_and_silent_close(self):
        cases = {
            "unknown_type": ("00000019444d320001040123456789abcdef0123456789abcdef026e31", 2),
            "unknown_replica": ("00000031444d320001010123456789abcdef0123456789abcdef026e34"
                                "000000000000000300000000000000000000000000000000", 4),
            "malformed": ("00000031444d320001010123456789abcdef0123456789abcdef026e31"
                          "000000000000000000000000000000000000000000000000", 3),
        }
        for name, (hex_frame, code) in cases.items():
            with self.subTest(name=name):
                answer = dm2.decode(dm2.parse_frame(self.raw(bytes.fromhex(hex_frame))))
                self.assertEqual(answer.payload, dm2.ErrorMsg(code))
        for name, data in {"magic": b"\x00\x00\x00\x08XYZ\x00abcd", "response": None}.items():
            if data is None:
                response = dm2.Message(dm2.Header(1, dm2.VOTE_RESPONSE, node.new_request_id(), "n1"),
                                       dm2.VoteResponse(3, True))
                data = dm2.encode_frame(response)
            with self.subTest(name=name):
                self.assertEqual(self.raw(data), b"")

    def test_server_closes_after_read_budget(self):
        start = time.monotonic()
        with socket.create_connection(self.endpoint, timeout=5) as conn:
            conn.sendall(b"\x00\x00")  # prefijo incompleto
            self.assertEqual(conn.recv(10), b"")
        self.assertLess(time.monotonic() - start, self.timeout + 1.0)

    def test_connection_limit(self):
        idle = [socket.create_connection(self.endpoint, timeout=5) for _ in range(self.max_connections)]
        try:
            time.sleep(0.2)
            with socket.create_connection(self.endpoint, timeout=5) as extra:
                extra.settimeout(0.5)
                self.assertEqual(extra.recv(10), b"", "la conexión excedente debe cerrarse")
        finally:
            for conn in idle:
                conn.close()
        time.sleep(0.2)
        self.assertEqual(self.client.ping(self.endpoint, "n1"), "n2", "el cupo se libera al cerrar")


class FakePeer:
    """Servidor de un solo uso que responde lo que el test decida a partir de la solicitud."""

    def __init__(self, respond):
        self.listener = socket.create_server((LOCAL, 0))
        self.listener.settimeout(5)
        self.endpoint = (LOCAL, self.listener.getsockname()[1])
        self.respond = respond
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        try:
            conn, _ = self.listener.accept()
            with conn:
                request = dm2.decode(node.read_frame(conn, 5))
                answer = self.respond(request)
                if answer is not None:
                    conn.sendall(dm2.frame(answer))
                else:
                    time.sleep(3)
        except OSError:
            pass
        finally:
            self.listener.close()


class ClientValidationTest(unittest.TestCase):
    def call(self, respond, timeout=1.0):
        peer = FakePeer(respond)
        return node.TcpClient(timeout=timeout).call_rpc(peer.endpoint, "n2", "n1", dm2.RequestVote(3, 0, 0))

    def test_accepts_correlated_response(self):
        response = self.call(lambda req: dm2.reply(req.header, dm2.VoteResponse(3, True), "n2"))
        self.assertTrue(response.payload.vote_granted)

    def test_rejects_mismatches(self):
        def other_id(req):
            header = dm2.Header(req.header.algorithm, dm2.VOTE_RESPONSE, b"\xfe" + req.header.request_id[1:], "n2")
            return dm2.encode(dm2.Message(header, dm2.VoteResponse(3, True)))

        def other_algorithm(req):
            header = dm2.Header(2, dm2.ERROR, req.header.request_id, "n2")
            return dm2.encode(dm2.Message(header, dm2.ErrorMsg(1)))

        def wrong_type(req):
            return dm2.reply(req.header, dm2.AppendResponse(3, False, 0), "n2")

        cases = {
            "requestId distinto": other_id,
            "algoritmo distinto": other_algorithm,
            "tipo de respuesta": wrong_type,
            "emisor inesperado": lambda req: dm2.reply(req.header, dm2.VoteResponse(3, True), "n3"),
            "respuesta DM1": lambda req: b"DM1|PONG|n2|" + b"a" * 32,
            "término obsoleto": lambda req: dm2.reply(req.header, dm2.VoteResponse(2, False), "n2"),
        }
        for name, respond in cases.items():
            with self.subTest(name=name), self.assertRaises(node.TransportError):
                self.call(respond)

    def test_client_timeout(self):
        start = time.monotonic()
        with self.assertRaises(OSError):
            self.call(lambda req: None, timeout=0.5)
        self.assertLess(time.monotonic() - start, 1.5)


@unittest.skipUnless(shutil.which("javac") and shutil.which("java"), "requiere JDK")
class JavaInteropTest(unittest.TestCase):
    """Java↔Python en DM2 binario con NodeMain real y --data-dir temporal."""

    @classmethod
    def setUpClass(cls):
        from build_all import build_java
        build_java()

    def start_java(self, peer_id: str, peer_port: int):
        java_port = free_port()
        data = Path(tempfile.mkdtemp(prefix="dm2-java-"))
        self.addCleanup(shutil.rmtree, data, True)
        process = subprocess.Popen(
            ["java", "-cp", str(ROOT / "build" / "java"), "dogmessenger.NodeMain", "--id", "n1",
             "--host", LOCAL, "--port", str(java_port), "--data-dir", str(data),
             "--peer", f"{peer_id}={LOCAL}:{peer_port}"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        self.addCleanup(self.stop, process)
        lines = queue.Queue()
        threading.Thread(target=lambda: [lines.put(l) for l in process.stdout], daemon=True).start()
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                if "LISTEN" in lines.get(timeout=0.5):
                    return java_port, lines
            except queue.Empty:
                self.assertIsNone(process.poll(), "Java terminó antes de escuchar")
        self.fail("Java no empezó a escuchar")

    @staticmethod
    def stop(process):
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        process.stdout.close()

    def test_python_request_vote_to_java(self):
        java_port, _ = self.start_java("n9", free_port())
        response = node.TcpClient().call_rpc((LOCAL, java_port), "n1", "n9", dm2.RequestVote(1000, 0, 0))
        self.assertEqual(response.header.sender_id, "n1")
        self.assertEqual(response.payload, dm2.VoteResponse(1000, True))
        print(f"\n  Python n9 -> Java n1: {response.payload}", file=sys.stderr)

    def test_java_request_vote_to_python(self):
        handler = VoteStub()
        server = node.TcpServer(LOCAL, 0, node.Dispatcher("n2", {"n1", "n2"}, handler))
        server.start()
        self.addCleanup(server.close)
        _, lines = self.start_java("n2", server.port)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                line = lines.get(timeout=0.5)
            except queue.Empty:
                continue
            # Java solo llega a líder (2 de 2) si validó y aceptó el voto DM2 de Python.
            if "role=LEADER" in line:
                break
        else:
            self.fail("Java no ganó la elección con el voto de Python")
        time.sleep(0.5)
        with handler.lock:
            kinds = {kind for kind, sender, _ in handler.seen if sender == "n1"}
        self.assertEqual(kinds, {"vote", "append"})
        print(f"\n  Java n1 -> Python n2: {line.strip()}", file=sys.stderr)


if __name__ == "__main__":
    unittest.main()
