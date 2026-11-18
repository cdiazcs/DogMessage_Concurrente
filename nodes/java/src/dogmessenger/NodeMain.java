package dogmessenger;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/** Punto de entrada y componentes TCP. Los builds del starter compilan este archivo directamente. */
public final class NodeMain {
    private static final Pattern NODE_ID = Pattern.compile("[a-zA-Z0-9_-]{1,32}");
    private static final Pattern DM1_REQUEST_ID = Pattern.compile("[a-fA-F0-9]{32}");
    private static final int DM1_MAGIC = 0x444d317c;

    private NodeMain() {}

    public static void main(String[] args) {
        String id = null, host = "0.0.0.0", target = null, configuredMembers = null;
        int port = 9101;
        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--id" -> id = args[++i];
                    case "--host" -> host = args[++i];
                    case "--port" -> port = Integer.parseInt(args[++i]);
                    case "--ping" -> target = args[++i];
                    case "--members" -> configuredMembers = args[++i];
                    default -> throw new IllegalArgumentException("Argumento desconocido " + args[i]);
                }
            }
            requireId(id);
            if (target != null) {
                int separator = target.lastIndexOf(':');
                if (separator <= 0) throw new IllegalArgumentException("Usa --ping IP:PUERTO");
                var address = new InetSocketAddress(target.substring(0, separator),
                        Integer.parseInt(target.substring(separator + 1)));
                String peer = new TcpClient().ping(address, id);
                System.out.println("OK " + id + " -> " + target + ": PONG de " + peer);
                return;
            }

            Set<String> members = configuredMembers == null ? Set.of(id)
                    : Set.copyOf(Arrays.asList(configuredMembers.split(",", -1)));
            var dispatcher = new RpcDispatcher(id, members, new RpcHandler() {});
            try (var server = new TcpServer(new InetSocketAddress(host, port), dispatcher, 16, 32)) {
                System.out.println("[JAVA " + id + "] LISTEN " + host + ":" + server.port() + " protocol=DM1+DM2");
                server.serve();
            }
        } catch (Exception error) {
            System.err.println("ERROR " + error.getMessage());
            System.exit(1);
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalArgumentException(message);
    }

    private static boolean validId(String value) {
        return value != null && NODE_ID.matcher(value).matches();
    }

    private static void requireId(String value) {
        require(validId(value), "ID obligatorio (1-32: letras ASCII, números, _ o -)");
    }

    private static boolean hasMagic(ByteBuffer body, int magic) {
        return body.remaining() >= Integer.BYTES && body.getInt(body.position()) == magic;
    }

    /** UUID como contenedor de 128 bits, sin validar versión ni variante en el wire. */
    private static void requireRequestId(UUID value) {
        require(value != null && (value.getMostSignificantBits() != 0 || value.getLeastSignificantBits() != 0),
                "requestId nulo");
    }

    public record Header(int algorithm, int type, UUID requestId, String senderId) {
        public Header {
            require(algorithm >= 0 && algorithm <= 255 && type >= 0 && type <= 255, "Código fuera de u8");
            requireRequestId(requestId);
            requireId(senderId);
        }
    }

    public record LogPosition(long index, long term) {
        public LogPosition {
            require(index >= 0 && term >= 0 && (index == 0) == (term == 0), "Posición de log inválida");
        }
    }

    public sealed interface Payload permits RequestVote, VoteResponse, AppendEntries, AppendResponse,
            ClientCommand, ClientResult, RpcError {
        int type();
    }

    public record RequestVote(long term, LogPosition lastLog) implements Payload {
        public RequestVote {
            require(term > 0 && Objects.requireNonNull(lastLog).term() <= term, "Término de voto inválido");
        }
        @Override public int type() { return MessageCodec.REQUEST_VOTE; }
    }

    public record VoteResponse(long term, boolean voteGranted) implements Payload {
        public VoteResponse {
            require(term >= 0 && (!voteGranted || term > 0), "Término de respuesta de voto inválido");
        }
        @Override public int type() { return MessageCodec.VOTE_RESPONSE; }
    }

    public sealed interface LogEntry permits NoOpEntry, CommandEntry {
        long term();
    }

    public record NoOpEntry(long term) implements LogEntry {
        public NoOpEntry {
            require(term > 0, "Término de entrada inválido");
        }
    }

    public record CommandEntry(long term, String clientId, UUID clientRequestId,
                               ClientCommand command) implements LogEntry {
        public CommandEntry {
            require(term > 0, "Término de entrada inválido");
            requireId(clientId);
            requireRequestId(clientRequestId);
            Objects.requireNonNull(command);
        }
    }

    public record AppendEntries(long term, LogPosition prevLog, long leaderCommit,
                                List<LogEntry> entries) implements Payload {
        public AppendEntries {
            require(term > 0 && Objects.requireNonNull(prevLog).term() <= term, "Término de réplica inválido");
            require(leaderCommit >= 0 && entries.size() <= 256, "Commit o cantidad de entradas inválidos");
            entries = List.copyOf(entries);
            require(prevLog.index() <= Long.MAX_VALUE - entries.size(), "Índice fuera de R64");
            long previousTerm = prevLog.term();
            for (LogEntry entry : entries) {
                require(entry.term() >= previousTerm && entry.term() <= term, "Orden de términos inválido");
                previousTerm = entry.term();
            }
        }
        public long lastIndex() { return prevLog.index() + entries.size(); }
        @Override public int type() { return MessageCodec.APPEND_ENTRIES; }
    }

    public record AppendResponse(long term, boolean success, long matchIndex) implements Payload {
        public AppendResponse {
            require(term >= 0 && matchIndex >= 0 && (success ? term > 0 : matchIndex == 0),
                    "Respuesta AppendEntries inválida");
        }
        @Override public int type() { return MessageCodec.APPEND_RESPONSE; }
    }

    public record ClientCommand(byte[] bytes) implements Payload {
        public ClientCommand {
            require(bytes.length >= 1 && bytes.length <= 65_536, "Longitud de comando inválida");
            bytes = bytes.clone();
        }
        @Override public byte[] bytes() { return bytes.clone(); }
        @Override public boolean equals(Object other) {
            return other instanceof ClientCommand command && Arrays.equals(bytes, command.bytes);
        }
        @Override public int hashCode() { return Arrays.hashCode(bytes); }
        @Override public int type() { return MessageCodec.CLIENT_COMMAND; }
    }

    public record ClientResult(int status, long logIndex, String leaderHint) implements Payload {
        public ClientResult {
            require(status >= 0 && status <= 3 && (status == 0 ? logIndex > 0 : logIndex == 0),
                    "Estado o índice de resultado inválido");
            require(leaderHint == null || (status == 1 && validId(leaderHint)), "leaderHint inválido");
        }
        @Override public int type() { return MessageCodec.CLIENT_RESULT; }
    }

    public enum ErrorCode {
        UNSUPPORTED_ALGORITHM(1), UNKNOWN_TYPE(2), MALFORMED_PAYLOAD(3), UNKNOWN_MEMBER(4);
        private final int wire;
        ErrorCode(int wire) { this.wire = wire; }
        public int wire() { return wire; }

        private static ErrorCode fromWire(int value) {
            for (ErrorCode code : values()) {
                if (code.wire == value) return code;
            }
            throw new IllegalArgumentException("Código ERROR desconocido");
        }
    }

    public record RpcError(ErrorCode code) implements Payload {
        public RpcError { Objects.requireNonNull(code); }
        @Override public int type() { return MessageCodec.ERROR; }
    }

    public record Message(Header header, Payload payload) {
        public Message {
            require(header.type() == payload.type(), "Tipo y payload incompatibles");
            require(payload instanceof RpcError || header.algorithm() == MessageCodec.RAFT, "Algoritmo no soportado");
        }
    }

    /** Codifica el cuerpo DM2. El prefijo TCP pertenece a la capa de framing. */
    public static final class MessageCodec {
        public static final int MAX_BODY = 1_048_576;
        public static final int RAFT = 1;
        public static final int REQUEST_VOTE = 0x01, APPEND_ENTRIES = 0x02, CLIENT_COMMAND = 0x03;
        public static final int VOTE_RESPONSE = 0x81, APPEND_RESPONSE = 0x82, CLIENT_RESULT = 0x83, ERROR = 0xff;
        private static final int MAGIC = 0x444d3200;

        private MessageCodec() {}

        public static Message decode(byte[] body) throws IOException {
            ByteBuffer input = ByteBuffer.wrap(body);
            Header header = readHeader(input);
            return new Message(header, readPayload(header, input));
        }

        private static Header readHeader(ByteBuffer input) throws IOException {
            if (input.remaining() > MAX_BODY || !hasMagic(input, MAGIC)) {
                throw new IOException("Firma o longitud DM2 inválida");
            }
            try {
                input.getInt();
                return new Header(u8(input), u8(input), readId(input), readNode(input, false));
            } catch (IllegalArgumentException | BufferUnderflowException error) {
                throw new IOException("Encabezado DM2 inválido", error);
            }
        }

        private static Payload readPayload(Header header, ByteBuffer input) throws IOException {
            try {
                require(header.type() == ERROR || header.algorithm() == RAFT, "Algoritmo no soportado");
                Payload payload = switch (header.type()) {
                    case REQUEST_VOTE -> new RequestVote(input.getLong(), readPosition(input));
                    case VOTE_RESPONSE -> new VoteResponse(input.getLong(), readBool(input));
                    case APPEND_ENTRIES -> readAppend(input);
                    case APPEND_RESPONSE -> new AppendResponse(input.getLong(), readBool(input), input.getLong());
                    case CLIENT_COMMAND -> readCommand(input);
                    case CLIENT_RESULT -> new ClientResult(u8(input), input.getLong(), readNode(input, true));
                    case ERROR -> new RpcError(ErrorCode.fromWire(u8(input)));
                    default -> throw new IllegalArgumentException("Tipo DM2 desconocido");
                };
                require(!input.hasRemaining(), "Bytes sobrantes");
                return payload;
            } catch (IllegalArgumentException | BufferUnderflowException error) {
                throw new IOException("Payload DM2 inválido", error);
            }
        }

        private static AppendEntries readAppend(ByteBuffer input) {
            long term = input.getLong();
            LogPosition previous = readPosition(input);
            long commit = input.getLong();
            int count = Short.toUnsignedInt(input.getShort());
            require(count <= 256 && count <= input.remaining() / 9, "Cantidad de entradas inválida");
            List<LogEntry> entries = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                long entryTerm = input.getLong();
                LogEntry entry = switch (u8(input)) {
                    case 0 -> new NoOpEntry(entryTerm);
                    case 1 -> new CommandEntry(entryTerm, readNode(input, false), readId(input), readCommand(input));
                    default -> throw new IllegalArgumentException("Tipo de entrada inválido");
                };
                entries.add(entry);
            }
            return new AppendEntries(term, previous, commit, entries);
        }

        public static byte[] encode(Message message) throws IOException {
            Header header = message.header();
            Payload payload = message.payload();
            int size = 23 + header.senderId().length() + payloadSize(payload);
            if (size > MAX_BODY) throw new IOException("Cuerpo DM2 excede el máximo");
            ByteBuffer output = ByteBuffer.allocate(size);
            output.putInt(MAGIC).put((byte) header.algorithm()).put((byte) header.type());
            putId(output, header.requestId());
            putNode(output, header.senderId());
            if (payload instanceof RequestVote vote) {
                output.putLong(vote.term());
                putPosition(output, vote.lastLog());
            } else if (payload instanceof VoteResponse vote) {
                output.putLong(vote.term()).put((byte) (vote.voteGranted() ? 1 : 0));
            } else if (payload instanceof AppendEntries append) {
                output.putLong(append.term());
                putPosition(output, append.prevLog());
                output.putLong(append.leaderCommit()).putShort((short) append.entries().size());
                for (LogEntry entry : append.entries()) {
                    output.putLong(entry.term());
                    if (entry instanceof CommandEntry command) {
                        output.put((byte) 1);
                        putNode(output, command.clientId());
                        putId(output, command.clientRequestId());
                        putCommand(output, command.command());
                    } else {
                        output.put((byte) 0);
                    }
                }
            } else if (payload instanceof AppendResponse append) {
                output.putLong(append.term()).put((byte) (append.success() ? 1 : 0)).putLong(append.matchIndex());
            } else if (payload instanceof ClientCommand command) {
                putCommand(output, command);
            } else if (payload instanceof ClientResult result) {
                output.put((byte) result.status()).putLong(result.logIndex());
                putNode(output, result.leaderHint());
            } else if (payload instanceof RpcError error) {
                output.put((byte) error.code().wire());
            }
            return output.array();
        }

        private static int payloadSize(Payload payload) {
            return switch (payload.type()) {
                case REQUEST_VOTE -> 24;
                case VOTE_RESPONSE -> 9;
                case APPEND_RESPONSE -> 17;
                case CLIENT_COMMAND -> 4 + ((ClientCommand) payload).bytes.length;
                case CLIENT_RESULT -> {
                    String hint = ((ClientResult) payload).leaderHint();
                    yield 10 + (hint == null ? 0 : hint.length());
                }
                case APPEND_ENTRIES -> {
                    int size = 34;
                    for (LogEntry entry : ((AppendEntries) payload).entries()) {
                        size += 9;
                        if (entry instanceof CommandEntry command) {
                            size += 21 + command.clientId().length() + command.command().bytes.length;
                        }
                    }
                    yield size;
                }
                case ERROR -> 1;
                default -> throw new IllegalArgumentException("Tipo DM2 desconocido");
            };
        }

        private static int u8(ByteBuffer input) { return Byte.toUnsignedInt(input.get()); }

        private static boolean readBool(ByteBuffer input) {
            int value = u8(input);
            require(value <= 1, "Booleano fuera de rango");
            return value == 1;
        }

        private static UUID readId(ByteBuffer input) { return new UUID(input.getLong(), input.getLong()); }

        private static void putId(ByteBuffer output, UUID id) {
            output.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());
        }

        private static String readNode(ByteBuffer input, boolean optional) {
            int size = u8(input);
            if (size == 0 && optional) return null;
            require(size >= 1 && size <= 32, "Longitud de ID inválida");
            byte[] bytes = new byte[size];
            input.get(bytes);
            String name = new String(bytes, StandardCharsets.US_ASCII);
            requireId(name);
            return name;
        }

        private static void putNode(ByteBuffer output, String name) {
            if (name == null) {
                output.put((byte) 0);
            } else {
                output.put((byte) name.length()).put(name.getBytes(StandardCharsets.US_ASCII));
            }
        }

        private static LogPosition readPosition(ByteBuffer input) {
            return new LogPosition(input.getLong(), input.getLong());
        }

        private static void putPosition(ByteBuffer output, LogPosition position) {
            output.putLong(position.index()).putLong(position.term());
        }

        private static ClientCommand readCommand(ByteBuffer input) {
            int size = input.getInt();
            require(size >= 1 && size <= 65_536 && size <= input.remaining(), "Longitud de comando inválida");
            byte[] bytes = new byte[size];
            input.get(bytes);
            return new ClientCommand(bytes);
        }

        private static void putCommand(ByteBuffer output, ClientCommand command) {
            output.putInt(command.bytes.length).put(command.bytes);
        }
    }

    /** Invocado concurrentemente; null indica que el manejador no puede emitir una respuesta. */
    public interface RpcHandler {
        default VoteResponse requestVote(Header header, RequestVote vote) throws IOException { return null; }
        default AppendResponse appendEntries(Header header, AppendEntries append) throws IOException { return null; }
        default ClientResult clientCommand(Header header, ClientCommand command) throws IOException { return null; }
    }

    public static final class RpcDispatcher {
        private final String nodeId;
        private final Set<String> members;
        private final RpcHandler handler;

        public RpcDispatcher(String nodeId, Set<String> members, RpcHandler handler) {
            requireId(nodeId);
            this.nodeId = nodeId;
            this.members = Set.copyOf(members);
            this.members.forEach(NodeMain::requireId);
            require(this.members.contains(nodeId), "La membresía debe incluir al nodo local");
            this.handler = Objects.requireNonNull(handler);
        }

        /** Devuelve un cuerpo de respuesta o null para cerrar la conexión. */
        public byte[] dispatch(byte[] body) throws IOException {
            ByteBuffer input = ByteBuffer.wrap(body);
            if (hasMagic(input, DM1_MAGIC)) return handlePing(body);
            Header header;
            try {
                header = MessageCodec.readHeader(input);
            } catch (IOException invalidHeader) {
                return null;
            }
            int type = header.type();
            if (type == 0 || type >= 0x80) return null;
            if (header.algorithm() != MessageCodec.RAFT) {
                return reply(header, new RpcError(ErrorCode.UNSUPPORTED_ALGORITHM));
            }
            if (type != MessageCodec.REQUEST_VOTE && type != MessageCodec.APPEND_ENTRIES
                    && type != MessageCodec.CLIENT_COMMAND) {
                return reply(header, new RpcError(ErrorCode.UNKNOWN_TYPE));
            }

            Payload request;
            try {
                request = MessageCodec.readPayload(header, input);
            } catch (IOException invalidPayload) {
                return reply(header, new RpcError(ErrorCode.MALFORMED_PAYLOAD));
            }
            if (!(request instanceof ClientCommand) && !members.contains(header.senderId())) {
                return reply(header, new RpcError(ErrorCode.UNKNOWN_MEMBER));
            }

            Payload response;
            if (request instanceof RequestVote vote) {
                response = handler.requestVote(header, vote);
            } else if (request instanceof AppendEntries append) {
                response = handler.appendEntries(header, append);
            } else {
                response = handler.clientCommand(header, (ClientCommand) request);
            }
            if (response == null) return null;
            validateResponse(request, response);
            if (response instanceof ClientResult result && result.leaderHint() != null
                    && !members.contains(result.leaderHint())) throw new IOException("Líder sugerido desconocido");
            return reply(header, response);
        }

        private byte[] reply(Header request, Payload response) throws IOException {
            return MessageCodec.encode(new Message(
                    new Header(request.algorithm(), response.type(), request.requestId(), nodeId), response));
        }

        private byte[] handlePing(byte[] body) {
            String[] parts = new String(body, StandardCharsets.UTF_8).split("\\|", -1);
            String response;
            if (parts.length == 4 && parts[1].equals("PING") && validId(parts[2])
                    && DM1_REQUEST_ID.matcher(parts[3]).matches()) {
                response = "DM1|PONG|" + nodeId + "|" + parts[3];
            } else {
                response = "DM1|ERROR|" + nodeId + "|00000000000000000000000000000000";
            }
            return response.getBytes(StandardCharsets.US_ASCII);
        }
    }

    private static void validateResponse(Payload request, Payload response) throws IOException {
        if (response instanceof RpcError) return;
        if (response.type() != (request.type() | 0x80)) throw new IOException("Tipo de respuesta inesperado");
        if (request instanceof RequestVote vote && response instanceof VoteResponse result) {
            if (result.term() < vote.term()) throw new IOException("Término de voto obsoleto");
        } else if (request instanceof AppendEntries append && response instanceof AppendResponse result) {
            if (result.term() < append.term() || (result.success() && result.matchIndex() != append.lastIndex())) {
                throw new IOException("Término o matchIndex inesperado");
            }
        }
    }

    /** Una conexión nueva por intento. El nodo esperado procede de la configuración del llamador. */
    public static final class TcpClient {
        public Message call(InetSocketAddress endpoint, String expectedPeer, Message request) throws IOException {
            requireId(expectedPeer);
            require(request.payload() instanceof RequestVote || request.payload() instanceof AppendEntries
                    || request.payload() instanceof ClientCommand, "Se requiere una solicitud DM2");
            byte[] outgoing = MessageCodec.encode(request);
            Message response = MessageCodec.decode(exchange(endpoint, outgoing));
            Header sent = request.header(), received = response.header();
            if (!sent.requestId().equals(received.requestId()) || sent.algorithm() != received.algorithm()
                    || !expectedPeer.equals(received.senderId())) {
                throw new IOException("Respuesta DM2 sin correlación");
            }
            validateResponse(request.payload(), response.payload());
            return response;
        }

        public Message call(InetSocketAddress endpoint, String expectedPeer, String senderId, Payload rpc)
                throws IOException {
            require(rpc instanceof RequestVote || rpc instanceof AppendEntries,
                    "Los comandos de cliente requieren un requestId explícito para reintentos");
            return call(endpoint, expectedPeer,
                    new Message(new Header(MessageCodec.RAFT, rpc.type(), UUID.randomUUID(), senderId), rpc));
        }

        public String ping(InetSocketAddress endpoint, String senderId) throws IOException {
            requireId(senderId);
            String requestId = UUID.randomUUID().toString().replace("-", "");
            byte[] outgoing = ("DM1|PING|" + senderId + "|" + requestId).getBytes(StandardCharsets.US_ASCII);
            String[] parts = new String(exchange(endpoint, outgoing), StandardCharsets.UTF_8).split("\\|", -1);
            if (parts.length != 4 || !parts[0].equals("DM1") || !parts[1].equals("PONG")
                    || !validId(parts[2]) || !requestId.equals(parts[3])) {
                throw new IOException("Respuesta PONG inesperada");
            }
            return parts[2];
        }

        private byte[] exchange(InetSocketAddress endpoint, byte[] body) throws IOException {
            try (SocketChannel socket = SocketChannel.open()) {
                socket.socket().connect(endpoint, 5_000);
                try (var connection = new FramedConnection(socket)) {
                    connection.write(body);
                    return connection.read();
                }
            }
        }
    }

    public static final class TcpServer implements AutoCloseable {
        private final ServerSocketChannel listener;
        private final RpcDispatcher dispatcher;
        private final ThreadPoolExecutor workers;
        private final Set<SocketChannel> clients = new HashSet<>();
        private boolean closed;

        public TcpServer(InetSocketAddress address, RpcDispatcher dispatcher, int threads, int queued)
                throws IOException {
            require(threads > 0 && queued > 0, "Capacidades TCP inválidas");
            this.dispatcher = Objects.requireNonNull(dispatcher);
            workers = new ThreadPoolExecutor(threads, threads, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(queued));
            listener = ServerSocketChannel.open();
            try {
                listener.socket().setReuseAddress(true);
                listener.bind(address);
            } catch (IOException | RuntimeException failure) {
                try {
                    listener.close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }

        public int port() throws IOException {
            return ((InetSocketAddress) listener.getLocalAddress()).getPort();
        }

        public void serve() throws IOException {
            while (listener.isOpen()) {
                SocketChannel client;
                try {
                    client = listener.accept();
                } catch (ClosedChannelException stopped) {
                    if (!listener.isOpen()) return;
                    throw stopped;
                }
                submit(client);
            }
        }

        private synchronized void submit(SocketChannel client) throws IOException {
            if (closed) {
                client.close();
                return;
            }
            clients.add(client);
            try {
                workers.execute(() -> handle(client));
            } catch (RejectedExecutionException saturated) {
                clients.remove(client);
                client.close();
            }
        }

        private void handle(SocketChannel client) {
            try (client; var connection = new FramedConnection(client)) {
                byte[] response = dispatcher.dispatch(connection.read());
                if (response != null) connection.write(response);
            } catch (IOException | CancelledKeyException failure) {
                // Un fallo de transporte o del manejador termina únicamente este intento.
            } finally {
                synchronized (this) {
                    clients.remove(client);
                }
            }
        }

        @Override public synchronized void close() throws IOException {
            if (closed) return;
            closed = true;
            IOException failure = null;
            try {
                listener.close();
            } catch (IOException error) {
                failure = error;
            }
            for (SocketChannel client : clients) {
                try {
                    client.close();
                } catch (IOException error) {
                    if (failure == null) failure = error;
                    else failure.addSuppressed(error);
                }
            }
            clients.clear();
            workers.shutdownNow();
            if (failure != null) throw failure;
        }
    }

    /** Un presupuesto monotónico por trama completa, compartido por prefijo y cuerpo. */
    private static final class FramedConnection implements AutoCloseable {
        private static final long TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(5);
        private final SocketChannel socket;
        private final Selector selector;
        private final SelectionKey key;

        FramedConnection(SocketChannel socket) throws IOException {
            this.socket = socket;
            socket.configureBlocking(false);
            selector = Selector.open();
            try {
                key = socket.register(selector, 0);
            } catch (IOException | RuntimeException failure) {
                try {
                    selector.close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }

        byte[] read() throws IOException {
            long deadline = System.nanoTime() + TIMEOUT_NANOS;
            ByteBuffer prefix = ByteBuffer.allocate(4);
            transfer(prefix, deadline, false);
            ByteBuffer body = ByteBuffer.allocate(checkedLength(prefix.getInt(0)));
            transfer(body, deadline, false);
            return body.array();
        }

        void write(byte[] body) throws IOException {
            int length = checkedLength(body.length);
            long deadline = System.nanoTime() + TIMEOUT_NANOS;
            ByteBuffer prefix = ByteBuffer.allocate(4).putInt(length).flip();
            transfer(prefix, deadline, true);
            transfer(ByteBuffer.wrap(body), deadline, true);
        }

        private static int checkedLength(int length) throws IOException {
            if (length < 1 || length > MessageCodec.MAX_BODY) throw new IOException("Longitud de trama inválida");
            return length;
        }

        private void transfer(ByteBuffer bytes, long deadline, boolean writing) throws IOException {
            key.interestOps(writing ? SelectionKey.OP_WRITE : SelectionKey.OP_READ);
            while (bytes.hasRemaining()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) throw new SocketTimeoutException("Plazo de trama agotado");
                if (Thread.currentThread().isInterrupted() || !key.isValid()) throw new ClosedChannelException();
                int count = writing ? socket.write(bytes) : socket.read(bytes);
                if (count < 0) throw new EOFException("Trama incompleta");
                if (count == 0) {
                    selector.select(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
                    selector.selectedKeys().clear();
                }
            }
        }

        @Override public void close() throws IOException { selector.close(); }
    }
}
