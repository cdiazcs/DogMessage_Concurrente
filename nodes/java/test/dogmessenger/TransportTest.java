package dogmessenger;

import static dogmessenger.NodeMain.*;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/** Pruebas ejecutables desde la raíz del repositorio con Java 17. */
public final class TransportTest {
    private static final HexFormat HEX = HexFormat.of();
    private static final UUID ID = new UUID(0x0123456789abcdefL, 0x0123456789abcdefL);
    private static final UUID COMMAND_ID = new UUID(0xfedcba9876543210L, 0xfedcba9876543210L);
    private static final LogPosition ORIGIN = new LogPosition(0, 0);
    private static final ClientCommand COMMAND = new ClientCommand(HEX.parseHex("00ff80"));
    private static final AppendEntries BATCH = new AppendEntries(4, new LogPosition(7, 3), 9,
            List.of(new NoOpEntry(4), new CommandEntry(4, "c1", COMMAND_ID, COMMAND)));

    private record Vector(String name, String direction, byte[] frame, String action, byte[] reply) {}

    @FunctionalInterface
    private interface IoAction { void run() throws Exception; }

    private static final class TestNode implements AutoCloseable {
        final AtomicInteger calls = new AtomicInteger();
        final ExecutorService acceptor = Executors.newSingleThreadExecutor();
        final TcpServer server;
        final Future<?> serving;
        final InetSocketAddress address;

        TestNode() throws IOException {
            this(new RpcHandler() {
                @Override public VoteResponse requestVote(Header header, RequestVote vote) {
                    return new VoteResponse(vote.term(), true);
                }

                @Override public AppendResponse appendEntries(Header header, AppendEntries append) {
                    return new AppendResponse(append.term(), true, append.lastIndex());
                }

                @Override public ClientResult clientCommand(Header header, ClientCommand command) {
                    if (!COMMAND.equals(command)) throw new AssertionError("Comando mutado");
                    return new ClientResult(2, 0, null);
                }
            }, 4, 16);
        }

        TestNode(RpcHandler delegate, int threads, int queued) throws IOException {
            RpcHandler observed = new RpcHandler() {
                @Override public VoteResponse requestVote(Header header, RequestVote vote) throws IOException {
                    calls.incrementAndGet();
                    return delegate.requestVote(header, vote);
                }

                @Override public AppendResponse appendEntries(Header header, AppendEntries append) throws IOException {
                    calls.incrementAndGet();
                    return delegate.appendEntries(header, append);
                }

                @Override public ClientResult clientCommand(Header header, ClientCommand command) throws IOException {
                    calls.incrementAndGet();
                    return delegate.clientCommand(header, command);
                }
            };
            server = new TcpServer(local(0), new RpcDispatcher("n2", Set.of("n1", "n2", "n3"), observed),
                    threads, queued);
            address = local(server.port());
            serving = acceptor.submit(() -> {
                server.serve();
                return null;
            });
        }

        @Override public void close() throws IOException {
            try {
                server.close();
                serving.get(5, TimeUnit.SECONDS);
            } catch (Exception error) {
                throw new IOException("El servidor de prueba no cerró correctamente", error);
            } finally {
                acceptor.shutdownNow();
            }
        }
    }

    private static InetSocketAddress local(int port) { return new InetSocketAddress("127.0.0.1", port); }
    private static void check(boolean condition, String description) {
        if (!condition) throw new AssertionError(description);
    }

    private static void fails(IoAction action, String description) throws Exception {
        try {
            action.run();
        } catch (IOException expected) {
            return;
        }
        throw new AssertionError("Se esperaba fallo: " + description);
    }

    private static byte[] frame(byte[] body) {
        return ByteBuffer.allocate(4 + body.length).putInt(body.length).put(body).array();
    }

    private static byte[] body(byte[] frame) { return Arrays.copyOfRange(frame, 4, frame.length); }

    private static Message message(String sender, Payload payload) {
        return new Message(new Header(1, payload.type(), ID, sender), payload);
    }

    private static byte[] wire(String sender, Payload payload) throws IOException {
        return frame(MessageCodec.encode(message(sender, payload)));
    }

    private static String stringField(String input, String key) {
        var match = Pattern.compile("\"" + key + "\"\\s*:\\s*\"([^\"]*)\"").matcher(input);
        return match.find() ? match.group(1) : null;
    }

    // El fixture de fase I usa objetos de caso con un único objeto expected.
    private static List<Vector> fixtures() throws IOException {
        String json = Files.readString(Path.of("protocol/fixtures/dm2.json"));
        var records = Pattern.compile("(?ms)^  \\{\\s*\"name\".*?^  }").matcher(json);
        List<Vector> vectors = new ArrayList<>();
        while (records.find()) {
            String record = records.group();
            String reply = stringField(record, "responseHex");
            vectors.add(new Vector(stringField(record, "name"), stringField(record, "direction"),
                    HEX.parseHex(stringField(record, "hex")), stringField(record, "action"),
                    reply == null ? null : HEX.parseHex(reply)));
        }
        check(vectors.size() == 32, "Cantidad de fixtures");
        return vectors;
    }

    private static Payload expectedPayload(String name) {
        return switch (name) {
            case "vote_empty_log" -> new RequestVote(3, ORIGIN);
            case "vote_granted" -> new VoteResponse(3, true);
            case "vote_denied" -> new VoteResponse(4, false);
            case "heartbeat" -> new AppendEntries(4, new LogPosition(7, 3), 9, List.of());
            case "append_two_entries" -> BATCH;
            case "append_success" -> new AppendResponse(4, true, 9);
            case "append_rejected" -> new AppendResponse(4, false, 0);
            case "command_opaque" -> COMMAND;
            case "client_result_0" -> new ClientResult(0, 9, null);
            case "client_result_1" -> new ClientResult(1, 0, "n1");
            case "client_result_2" -> new ClientResult(2, 0, null);
            case "client_result_3" -> new ClientResult(3, 0, null);
            case "correlated_error" -> new RpcError(ErrorCode.MALFORMED_PAYLOAD);
            default -> throw new AssertionError("Vector válido sin oráculo: " + name);
        };
    }

    private static Socket connect(InetSocketAddress address) throws IOException {
        Socket socket = new Socket();
        try {
            socket.connect(address, 2_000);
            socket.setSoTimeout(7_000);
            socket.setTcpNoDelay(true);
            return socket;
        } catch (IOException error) {
            socket.close();
            throw error;
        }
    }

    private static byte[] receive(Socket socket) throws IOException {
        var input = new DataInputStream(socket.getInputStream());
        int length = input.readInt();
        check(length >= 1 && length <= MessageCodec.MAX_BODY, "Longitud de respuesta");
        byte[] received = new byte[length];
        input.readFully(received);
        check(input.read() == -1, "El servidor debe cerrar después de responder");
        return frame(received);
    }

    private static Message fromPeer(byte[] reply, Message pending, String peer) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (var listener = new ServerSocket()) {
            listener.bind(local(0));
            listener.setSoTimeout(7_000);
            Future<?> served = executor.submit(() -> {
                try (Socket socket = listener.accept()) {
                    socket.setSoTimeout(7_000);
                    var input = new DataInputStream(socket.getInputStream());
                    int length = input.readInt();
                    check(Arrays.equals(frame(input.readNBytes(length)), wire(pending.header().senderId(),
                            pending.payload())), "Trama enviada por TcpClient");
                    socket.getOutputStream().write(reply);
                }
                return null;
            });
            try {
                return new TcpClient().call(local(listener.getLocalPort()), peer, pending);
            } finally {
                served.get(8, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private static void verifyFixtures(TestNode node) throws Exception {
        for (Vector vector : fixtures()) {
            int before = node.calls.get();
            if (vector.action().equals("decode")) {
                Message decoded = MessageCodec.decode(body(vector.frame()));
                String sender = vector.direction().equals("client") ? "n2"
                        : vector.name().equals("command_opaque") ? "c1" : "n1";
                check(decoded.equals(message(sender, expectedPayload(vector.name()))), vector.name());
                check(Arrays.equals(frame(MessageCodec.encode(decoded)), vector.frame()), vector.name());
            }
            if (vector.direction().equals("server")) {
                try (Socket socket = connect(node.address)) {
                    socket.getOutputStream().write(vector.frame());
                    socket.shutdownOutput();
                    if (vector.action().equals("close")) {
                        check(socket.getInputStream().read() == -1, vector.name());
                    } else {
                        byte[] expected = switch (vector.name()) {
                            case "vote_empty_log" -> wire("n2", new VoteResponse(3, true));
                            case "heartbeat" -> wire("n2", new AppendResponse(4, true, 7));
                            case "append_two_entries" -> wire("n2", new AppendResponse(4, true, 9));
                            case "command_opaque" -> wire("n2", new ClientResult(2, 0, null));
                            default -> vector.reply();
                        };
                        check(Arrays.equals(receive(socket), expected), vector.name());
                    }
                }
                check(node.calls.get() == before + (vector.action().equals("decode") ? 1 : 0),
                        "Llamada al manejador: " + vector.name());
            } else {
                Payload request;
                if (vector.name().startsWith("append_")) request = BATCH;
                else if (vector.name().startsWith("client_result_")) request = COMMAND;
                else request = new RequestVote(3, ORIGIN);
                Message pending = message(request == COMMAND ? "c1" : "n1", request);
                if (vector.action().equals("discard")) {
                    fails(() -> fromPeer(vector.frame(), pending, "n2"), vector.name());
                } else {
                    check(fromPeer(vector.frame(), pending, "n2").payload().equals(expectedPayload(vector.name())),
                            vector.name());
                }
            }
        }
        System.out.println("OK 32 fixtures DM2 (codec, dispatcher y sockets)");
    }

    private static void checkCorrelations() throws Exception {
        Message vote = message("n1", new RequestVote(3, ORIGIN));
        fails(() -> fromPeer(wire("n3", new VoteResponse(3, true)), vote, "n2"), "emisor inesperado");
        fails(() -> fromPeer(wire("n2", new VoteResponse(2, false)), vote, "n2"), "término inferior");
        fails(() -> fromPeer(wire("n2", new AppendResponse(3, true, 0)), vote, "n2"), "tipo inesperado");
        byte[] wrongAlgorithm = wire("n2", new RpcError(ErrorCode.UNSUPPORTED_ALGORITHM));
        wrongAlgorithm[8] = 2;
        fails(() -> fromPeer(wrongAlgorithm, vote, "n2"), "algoritmo inesperado");
        fails(() -> fromPeer(wire("n2", new AppendResponse(4, true, 8)), message("n1", BATCH), "n2"),
                "matchIndex incorrecto");
        byte[] dm1 = frame("DM1|ERROR|n2|00000000000000000000000000000000".getBytes(StandardCharsets.US_ASCII));
        fails(() -> fromPeer(dm1, vote, "n2"), "respuesta DM1 para DM2");
        System.out.println("OK correlación adicional y rechazo de respuesta DM1 a DM2");
    }

    private static void checkLimits() throws Exception {
        byte[] large = new byte[65_536];
        var entry = new CommandEntry(1, "x", COMMAND_ID, new ClientCommand(large));
        var entries = new ArrayList<LogEntry>();
        for (int i = 0; i < 15; i++) entries.add(entry);
        // 59 bytes de encabezado y datos fijos, más 31 por entrada COMMAND.
        int rest = MessageCodec.MAX_BODY - 59 - 15 * (65_536 + 31) - 31;
        entries.add(new CommandEntry(1, "x", COMMAND_ID, new ClientCommand(new byte[rest])));
        Message max = message("n1", new AppendEntries(1, ORIGIN, 0, entries));
        byte[] encoded = MessageCodec.encode(max);
        check(encoded.length == MessageCodec.MAX_BODY && max.equals(MessageCodec.decode(encoded)), "Cuerpo máximo");
        entries.set(15, new CommandEntry(1, "x", COMMAND_ID, new ClientCommand(new byte[rest + 1])));
        fails(() -> MessageCodec.encode(message("n1", new AppendEntries(1, ORIGIN, 0, entries))), "Cuerpo +1");
        var batch = new AppendEntries(Long.MAX_VALUE, new LogPosition(Long.MAX_VALUE - 256, Long.MAX_VALUE),
                Long.MAX_VALUE, java.util.Collections.nCopies(256, new NoOpEntry(Long.MAX_VALUE)));
        Message boundary = message("a".repeat(32), batch);
        check(boundary.equals(MessageCodec.decode(MessageCodec.encode(boundary))), "R64, lote e ID máximos");
        System.out.println("OK límites de cuerpo, comando, lote, R64 e identificadores");
    }

    private static void checkStream(TestNode node) throws Exception {
        byte[] request = wire("n1", new RequestVote(3, ORIGIN));
        byte[] reply = wire("n2", new VoteResponse(3, true));
        try (Socket socket = connect(node.address)) {
            for (byte value : request) {
                socket.getOutputStream().write(value);
                Thread.sleep(2);
            }
            check(Arrays.equals(receive(socket), reply), "Fragmentación de solicitud");
        }
        int before = node.calls.get();
        try (Socket socket = connect(node.address)) {
            byte[] both = ByteBuffer.allocate(request.length * 2).put(request).put(request).array();
            socket.getOutputStream().write(both);
            socket.shutdownOutput();
            var input = new DataInputStream(socket.getInputStream());
            check(Arrays.equals(input.readNBytes(reply.length), reply), "Primera trama agrupada");
            // TCP puede reiniciar al cerrar si quedaron bytes de la segunda trama por leer.
            try {
                check(input.read() == -1, "Segunda trama no procesada");
            } catch (java.net.SocketException reset) {
                check(!socket.isClosed(), "El cierre se originó en el servidor");
            }
        }
        check(node.calls.get() == before + 1, "Una operación por conexión");
        try (Socket socket = connect(node.address)) {
            socket.getOutputStream().write(request, 0, 2);
        }
        check(new TcpClient().ping(node.address, "n1").equals("n2"), "Servicio tras desconexión");
        System.out.println("OK fragmentación, tramas agrupadas y desconexión");
    }

    private static void checkTimeout(TestNode node) throws Exception {
        byte[] request = wire("n1", new RequestVote(3, ORIGIN));
        long start = System.nanoTime();
        try (Socket socket = connect(node.address)) {
            socket.getOutputStream().write(request, 0, 2);
            Thread.sleep(3_000);
            socket.getOutputStream().write(request, 2, 4);
            check(socket.getInputStream().read() == -1, "Cierre por presupuesto total de lectura");
        }
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        check(elapsed >= 4_500 && elapsed < 6_800, "Plazo del servidor: " + elapsed + " ms");

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (var listener = new ServerSocket()) {
            listener.bind(local(0));
            listener.setSoTimeout(7_000);
            Future<?> served = executor.submit(() -> {
                try (Socket socket = listener.accept()) {
                    socket.setSoTimeout(7_000);
                    var input = new DataInputStream(socket.getInputStream());
                    input.readFully(new byte[input.readInt()]);
                    check(input.read() == -1, "El cliente agotó el plazo y cerró");
                }
                return null;
            });
            try {
                new TcpClient().call(local(listener.getLocalPort()), "n2", "n1", new RequestVote(3, ORIGIN));
                throw new AssertionError("Se esperaba timeout del cliente");
            } catch (SocketTimeoutException expected) {
                served.get(8, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }
        System.out.println("OK plazo monotónico del servidor y timeout/cierre del cliente");
    }

    private static void checkConcurrency() throws Exception {
        CountDownLatch active = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        RpcHandler blocker = new RpcHandler() {
            @Override public VoteResponse requestVote(Header header, RequestVote vote) throws IOException {
                active.countDown();
                try {
                    if (!release.await(6, TimeUnit.SECONDS)) throw new IOException("Prueba de saturación vencida");
                } catch (InterruptedException stopped) {
                    Thread.currentThread().interrupt();
                    throw new IOException(stopped);
                }
                return new VoteResponse(vote.term(), true);
            }
        };
        byte[] request = wire("n1", new RequestVote(3, ORIGIN));
        try (var node = new TestNode(blocker, 2, 1);
             Socket first = connect(node.address); Socket second = connect(node.address)) {
            try {
                first.getOutputStream().write(request);
                second.getOutputStream().write(request);
                check(active.await(3, TimeUnit.SECONDS), "Dos manejadores ejecutados en paralelo");
                try (Socket queued = connect(node.address); Socket excess = connect(node.address)) {
                    check(excess.getInputStream().read() == -1, "Rechazo al saturar el pool y la cola");
                    queued.getOutputStream().write(request);
                    release.countDown();
                    byte[] expected = wire("n2", new VoteResponse(3, true));
                    for (Socket socket : List.of(first, second, queued)) {
                        check(Arrays.equals(receive(socket), expected), "Respuesta concurrente");
                    }
                    check(node.calls.get() == 3, "Solo se procesan las conexiones admitidas");
                }
            } finally {
                release.countDown();
            }
        }
        System.out.println("OK clientes concurrentes, cola acotada y rechazo de sobrecarga");
    }

    private static void checkDm1(TestNode node) throws IOException {
        check(new TcpClient().ping(node.address, "n1").equals("n2"), "DM1 PING/PONG");
        try (Socket socket = connect(node.address)) {
            socket.getOutputStream().write(frame("DM1|PING|bad!|bad".getBytes(StandardCharsets.US_ASCII)));
            String response = new String(body(receive(socket)), StandardCharsets.US_ASCII);
            check(response.equals("DM1|ERROR|n2|00000000000000000000000000000000"), "DM1 ERROR");
        }
        System.out.println("OK DM1 PING/PONG y ERROR en el listener mixto");
    }

    public static void main(String[] args) throws Exception {
        try (var node = new TestNode()) {
            verifyFixtures(node);
            checkDm1(node);
            checkCorrelations();
            checkLimits();
            checkStream(node);
            checkConcurrency();
            checkTimeout(node);
        }
    }
}
