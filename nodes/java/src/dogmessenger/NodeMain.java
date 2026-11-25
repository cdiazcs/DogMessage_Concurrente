package dogmessenger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.zip.CRC32C;

/** Punto de entrada y componentes TCP. Los builds del starter compilan este archivo directamente. */
public final class NodeMain {
    private static final Pattern NODE_ID = Pattern.compile("[a-zA-Z0-9_-]{1,32}");
    private static final Pattern DM1_REQUEST_ID = Pattern.compile("[a-fA-F0-9]{32}");
    private static final int DM1_MAGIC = 0x444d317c;

    private NodeMain() {}

    public static void main(String[] args) {
        String id = null, host = "0.0.0.0", target = null, configuredMembers = null;
        Path dataDir = null;
        boolean inspect = false;
        Map<String, InetSocketAddress> peers = new HashMap<>();
        int port = 9101;
        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--id" -> id = args[++i];
                    case "--host" -> host = args[++i];
                    case "--port" -> port = Integer.parseInt(args[++i]);
                    case "--ping" -> target = args[++i];
                    case "--members" -> configuredMembers = args[++i];
                    case "--data-dir" -> dataDir = Path.of(args[++i]);
                    case "--inspect" -> inspect = true;
                    case "--peer" -> addPeer(peers, args[++i]);
                    default -> throw new IllegalArgumentException("Argumento desconocido " + args[i]);
                }
            }
            requireId(id);
            require(!inspect || (dataDir != null && target == null), "--inspect requiere --data-dir");
            if (target != null) {
                int separator = target.lastIndexOf(':');
                if (separator <= 0) throw new IllegalArgumentException("Usa --ping IP:PUERTO");
                var address = new InetSocketAddress(target.substring(0, separator),
                        Integer.parseInt(target.substring(separator + 1)));
                String peer = new TcpClient().ping(address, id);
                System.out.println("OK " + id + " -> " + target + ": PONG de " + peer);
                return;
            }

            Set<String> members = configuredMembers == null ? new HashSet<>(peers.keySet())
                    : Set.copyOf(Arrays.asList(configuredMembers.split(",", -1)));
            if (configuredMembers == null) members.add(id);
            InetSocketAddress listen = new InetSocketAddress(host, port);
            if (dataDir == null) {
                require(peers.isEmpty(), "--peer requiere --data-dir");
                serve(id, listen, members, new RpcHandler() {}, () -> {});
            } else {
                try (var raft = new RaftNode(id, members, peers, dataDir)) {
                    if (inspect) {
                        printRaftState(raft.snapshot());
                        return;
                    }
                    Thread shutdown = new Thread(raft::shutdown, "raft-shutdown");
                    Runtime.getRuntime().addShutdownHook(shutdown);
                    try {
                        serve(id, listen, members, raft, raft::start);
                    } finally {
                        try {
                            Runtime.getRuntime().removeShutdownHook(shutdown);
                        } catch (IllegalStateException stopping) {
                            // La JVM ya está ejecutando los hooks.
                        }
                    }
                }
            }
        } catch (Exception error) {
            System.err.println("ERROR " + error.getMessage());
            System.exit(1);
        }
    }

    private static void printRaftState(RaftNode.Snapshot snapshot) {
        System.out.printf("role=%s term=%d leader=%s commitIndex=%d lastApplied=%d lastLogIndex=%d appliedCommands=%s%n",
                snapshot.role(), snapshot.term(), snapshot.leader(), snapshot.commitIndex(), snapshot.lastApplied(),
                snapshot.log().size(), snapshot.appliedCommandIndexes());
        for (RaftLog.LogEntry entry : snapshot.log()) {
            System.out.printf("index=%d term=%d type=%s status=%s client=%s requestId=%s command=%s%n",
                    entry.index(), entry.term(), entry.type(),
                    entry.index() <= snapshot.commitIndex() ? "COMMITTED" : "PENDING",
                    entry.clientId(), entry.requestId(),
                    entry.type() == RaftLog.EntryType.COMMAND ? HexFormat.of().formatHex(entry.payload()) : "-");
        }
    }

    private static void serve(String id, InetSocketAddress listen, Set<String> members, RpcHandler handler,
                              Runnable start) throws IOException {
        var dispatcher = new RpcDispatcher(id, members, handler);
        try (var server = new TcpServer(listen, dispatcher, 16, 32)) {
            System.out.println("[JAVA " + id + "] LISTEN " + listen.getHostString() + ":" + server.port()
                    + " protocol=DM1+DM2");
            start.run();
            server.serve();
        }
    }

    private static void addPeer(Map<String, InetSocketAddress> peers, String argument) {
        int equals = argument.indexOf('=');
        int colon = argument.lastIndexOf(':');
        require(equals > 0 && colon > equals + 1, "Usa --peer ID=HOST:PUERTO");
        String peer = argument.substring(0, equals);
        requireId(peer);
        int port = Integer.parseInt(argument.substring(colon + 1));
        require(port > 0, "El puerto del peer debe ser positivo");
        var endpoint = new InetSocketAddress(argument.substring(equals + 1, colon), port);
        require(!endpoint.isUnresolved(), "Host de peer inválido: " + endpoint.getHostString());
        require(peers.putIfAbsent(peer, endpoint) == null, "Peer repetido: " + peer);
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
            require(leaderCommit >= 0 && entries.size() <= MessageCodec.MAX_APPEND_ENTRIES,
                    "Commit o cantidad de entradas inválidos");
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
        private static final int MAX_APPEND_ENTRIES = 256;
        private static final int HEADER_BYTES = 23; // Incluye la longitud de senderId.
        private static final int APPEND_FIXED_BYTES = 34;
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
            require(count <= MAX_APPEND_ENTRIES && count <= input.remaining() / 9, "Cantidad de entradas inválida");
            List<LogEntry> entries = new ArrayList<>(count);
            for (int i = 0; i < count; i++) entries.add(readEntry(input));
            return new AppendEntries(term, previous, commit, entries);
        }

        private static LogEntry readEntry(ByteBuffer input) {
            long term = input.getLong();
            return switch (u8(input)) {
                case 0 -> new NoOpEntry(term);
                case 1 -> new CommandEntry(term, readNode(input, false), readId(input), readCommand(input));
                default -> throw new IllegalArgumentException("Tipo de entrada inválido");
            };
        }

        private static void putEntry(ByteBuffer output, LogEntry entry) {
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

        private static int entrySize(LogEntry entry) {
            if (entry instanceof CommandEntry command) {
                return 30 + command.clientId().length() + command.command().bytes.length;
            }
            return 9;
        }

        public static byte[] encode(Message message) throws IOException {
            Header header = message.header();
            Payload payload = message.payload();
            int size = HEADER_BYTES + header.senderId().length() + payloadSize(payload);
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
                for (LogEntry entry : append.entries()) putEntry(output, entry);
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
                    int size = APPEND_FIXED_BYTES;
                    for (LogEntry entry : ((AppendEntries) payload).entries()) size += entrySize(entry);
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

    /** Log local. Solo publica entradas cuya escritura y sincronización terminaron. */
    public static final class RaftLog implements AutoCloseable {
        private static final LogPosition ORIGIN = new LogPosition(0, 0);

        public enum EntryType { NO_OP, COMMAND }

        /** El índice vive en disco. El contenido conserva la representación inmutable que usa DM2. */
        public record LogEntry(long index, NodeMain.LogEntry entry) {
            public LogEntry {
                require(index > 0, "Índice de entrada inválido");
                Objects.requireNonNull(entry);
            }

            public long term() { return entry.term(); }

            public EntryType type() {
                return entry instanceof CommandEntry ? EntryType.COMMAND : EntryType.NO_OP;
            }

            public String clientId() {
                return entry instanceof CommandEntry command ? command.clientId() : null;
            }

            public UUID requestId() {
                return entry instanceof CommandEntry command ? command.clientRequestId() : null;
            }

            public byte[] payload() {
                return entry instanceof CommandEntry command ? command.command().bytes() : new byte[0];
            }
        }

        private final LogStorage storage;
        private final List<LogEntry> entries;

        public RaftLog(Path directory) throws IOException {
            storage = new LogStorage(directory);
            try {
                entries = storage.recover();
            } catch (IOException | RuntimeException failure) {
                try {
                    storage.close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }

        public synchronized LogEntry append(NodeMain.LogEntry entry) throws IOException {
            LogPosition previous = lastPosition();
            if (previous.index() == Long.MAX_VALUE) throw new IOException("Índice del log agotado");
            LogEntry next = new LogEntry(previous.index() + 1, entry);
            require(next.term() >= previous.term(), "El término del log no puede retroceder");
            storage.append(next);
            entries.add(next);
            return next;
        }

        /** Devuelve null si el índice no identifica una entrada real. */
        public synchronized LogEntry entryAt(long index) {
            if (index < 1 || index > entries.size()) return null;
            return entries.get((int) index - 1);
        }

        public synchronized LogPosition lastPosition() {
            if (entries.isEmpty()) return ORIGIN;
            LogEntry last = entries.get(entries.size() - 1);
            return new LogPosition(last.index(), last.term());
        }

        public long lastLogIndex() { return lastPosition().index(); }
        public long lastLogTerm() { return lastPosition().term(); }

        synchronized boolean matches(LogPosition position) {
            if (position.equals(ORIGIN)) return true;
            LogEntry entry = entryAt(position.index());
            return entry != null && entry.term() == position.term();
        }

        synchronized boolean merge(AppendEntries append, long commitIndex) throws IOException {
            if (!matches(append.prevLog())) return false;
            int keep = (int) append.prevLog().index();
            List<NodeMain.LogEntry> incoming = append.entries();
            int firstNew = 0;
            while (firstNew < incoming.size() && keep < entries.size()
                    && entries.get(keep).term() == incoming.get(firstNew).term()) {
                keep++;
                firstNew++;
            }
            if (firstNew == incoming.size()) return true;
            if (keep < commitIndex) throw new IOException("Conflicto con el prefijo comprometido");

            List<LogEntry> replacement = new ArrayList<>(incoming.size() - firstNew);
            for (int i = firstNew; i < incoming.size(); i++) {
                replacement.add(new LogEntry(keep + (long) replacement.size() + 1, incoming.get(i)));
            }
            storage.replaceAfter(keep, replacement);
            entries.subList(keep, entries.size()).clear();
            entries.addAll(replacement);
            return true;
        }

        synchronized AppendEntries batch(long term, long nextIndex, String senderId, long commitIndex) {
            long previousIndex = nextIndex - 1;
            LogPosition previous = previousIndex == 0 ? ORIGIN
                    : new LogPosition(previousIndex, entries.get((int) (previousIndex - 1)).term());
            int remaining = MessageCodec.MAX_BODY - MessageCodec.HEADER_BYTES - senderId.length()
                    - MessageCodec.APPEND_FIXED_BYTES;
            List<NodeMain.LogEntry> batch = new ArrayList<>();
            for (int i = (int) previousIndex; i < entries.size() && batch.size() < MessageCodec.MAX_APPEND_ENTRIES; i++) {
                NodeMain.LogEntry entry = entries.get(i).entry();
                int size = MessageCodec.entrySize(entry);
                if (size > remaining) break;
                batch.add(entry);
                remaining -= size;
            }
            return new AppendEntries(term, previous, commitIndex, batch);
        }

        synchronized LogEntry findCommand(String clientId, UUID requestId) {
            for (LogEntry entry : entries) {
                if (entry.entry() instanceof CommandEntry command && command.clientId().equals(clientId)
                        && command.clientRequestId().equals(requestId)) return entry;
            }
            return null;
        }

        synchronized List<LogEntry> snapshot() { return List.copyOf(entries); }

        @Override public synchronized void close() throws IOException { storage.close(); }
    }

    /** Journal local con formato versionado por registro. Acceso bajo el monitor de RaftLog. */
    private static final class LogStorage implements AutoCloseable {
        private static final int MAGIC = 0x444c4731; // DLG1
        private static final int HEADER_SIZE = 3 * Integer.BYTES;
        private static final int MIN_BODY = Long.BYTES + 9;
        private static final int MAX_BODY = Long.BYTES + 30 + 32 + 65_536;
        private static final Set<Path> OPEN_DIRECTORIES = new HashSet<>();
        private final Path directory;
        private final FileChannel lockChannel;
        private final FileChannel channel;
        private final List<Long> offsets = new ArrayList<>();
        private boolean failed;
        private boolean closed;

        LogStorage(Path directory) throws IOException {
            Files.createDirectories(directory);
            this.directory = directory.toRealPath();
            synchronized (OPEN_DIRECTORIES) {
                if (!OPEN_DIRECTORIES.add(this.directory)) throw new IOException("Log Raft en uso: " + directory);
            }
            try {
                lockChannel = FileChannel.open(this.directory.resolve("raft-log.lock"),
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                try {
                    if (lockChannel.tryLock() == null) throw new IOException("Log Raft en uso: " + directory);
                    channel = openLogFile(this.directory);
                } catch (IOException | RuntimeException failure) {
                    closeAfterFailure(lockChannel, failure);
                    throw failure;
                }
            } catch (IOException | RuntimeException failure) {
                releaseDirectory();
                throw failure;
            }
        }

        private static FileChannel openLogFile(Path directory) throws IOException {
            FileChannel file = FileChannel.open(directory.resolve("raft-log.bin"), StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            try {
                syncDirectory(directory);
                return file;
            } catch (IOException | RuntimeException failure) {
                closeAfterFailure(file, failure);
                throw failure;
            }
        }

        private static void closeAfterFailure(FileChannel file, Exception failure) {
            try {
                file.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }

        private void releaseDirectory() {
            synchronized (OPEN_DIRECTORIES) {
                OPEN_DIRECTORIES.remove(directory);
            }
        }

        List<RaftLog.LogEntry> recover() throws IOException {
            List<RaftLog.LogEntry> entries = new ArrayList<>();
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
            long lastTerm = 0;
            long size = channel.size();
            while (channel.position() < size) {
                long start = channel.position();
                if (size - start < HEADER_SIZE) {
                    truncateTail(start);
                    break;
                }
                readFully(header.clear());
                int magic = header.getInt();
                int length = header.getInt();
                if (magic != MAGIC || length < MIN_BODY || length > MAX_BODY || header.getInt() != ~length) {
                    throw new IOException("Cabecera inválida en offset " + start);
                }
                if (size - channel.position() < length + Integer.BYTES) {
                    truncateTail(start);
                    break;
                }
                ByteBuffer body = readFully(ByteBuffer.allocate(length + Integer.BYTES));
                if (body.getInt(length) != checksum(body.array(), 0, length)) {
                    throw new IOException("CRC inválido en offset " + start);
                }
                body.limit(length);
                try {
                    var entry = new RaftLog.LogEntry(body.getLong(), MessageCodec.readEntry(body));
                    require(!body.hasRemaining() && entry.index() == entries.size() + 1L && entry.term() >= lastTerm,
                            "Secuencia o longitud inválida");
                    entries.add(entry);
                    offsets.add(start);
                    lastTerm = entry.term();
                } catch (IllegalArgumentException | BufferUnderflowException invalid) {
                    throw new IOException("Registro inválido en offset " + start, invalid);
                }
            }
            return entries;
        }

        void append(RaftLog.LogEntry entry) throws IOException {
            replaceAfter(offsets.size(), List.of(entry));
        }

        void replaceAfter(int keep, List<RaftLog.LogEntry> replacement) throws IOException {
            if (failed || !channel.isOpen()) throw new IOException("Log Raft cerrado o requiere recuperación");
            try {
                if (keep < offsets.size()) {
                    // Tras una caída, un prefijo más corto es válido y el líder reenviará el resto.
                    truncateTail(offsets.get(keep));
                    offsets.subList(keep, offsets.size()).clear();
                }
                for (RaftLog.LogEntry entry : replacement) {
                    long start = channel.position();
                    writeRecord(entry);
                    offsets.add(start);
                }
                channel.force(true);
            } catch (IOException failure) {
                failed = true;
                throw failure;
            }
        }

        private void writeRecord(RaftLog.LogEntry entry) throws IOException {
            int length = Long.BYTES + MessageCodec.entrySize(entry.entry());
            ByteBuffer record = ByteBuffer.allocate(HEADER_SIZE + length + Integer.BYTES);
            record.putInt(MAGIC).putInt(length).putInt(~length).putLong(entry.index());
            MessageCodec.putEntry(record, entry.entry());
            record.putInt(checksum(record.array(), HEADER_SIZE, length)).flip();
            while (record.hasRemaining()) channel.write(record);
        }

        private ByteBuffer readFully(ByteBuffer target) throws IOException {
            while (target.hasRemaining()) {
                if (channel.read(target) < 0) throw new EOFException("El log cambió durante la recuperación");
            }
            return target.flip();
        }

        private void truncateTail(long offset) throws IOException {
            channel.truncate(offset);
            channel.position(offset);
            channel.force(true);
        }

        private static int checksum(byte[] bytes, int offset, int length) {
            var crc = new CRC32C();
            crc.update(bytes, offset, length);
            return (int) crc.getValue();
        }

        @Override public void close() throws IOException {
            if (closed) return;
            closed = true;
            try {
                channel.close();
            } finally {
                try {
                    lockChannel.close();
                } finally {
                    releaseDirectory();
                }
            }
        }
    }

    private static void syncDirectory(Path directory) throws IOException {
        try (FileChannel parent = FileChannel.open(directory, StandardOpenOption.READ)) {
            parent.force(true);
        }
    }

    public enum RaftRole { FOLLOWER, CANDIDATE, LEADER }

    /** Gestionado bajo el monitor de RaftNode. Cada reinicio sortea un plazo nuevo. */
    static final class ElectionTimer {
        private long deadline;

        void reset() {
            long delay = ThreadLocalRandom.current().nextLong(500, 901);
            deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delay);
        }

        boolean expired(long now) { return now - deadline >= 0; }
    }

    public static final class RaftNode implements RpcHandler, AutoCloseable {
        private static final long HEARTBEAT_INTERVAL = TimeUnit.MILLISECONDS.toNanos(125);
        private static final long CLIENT_WAIT = TimeUnit.MILLISECONDS.toNanos(750);
        private static final int MAX_CLIENT_WAITERS = 8;
        private final String id;
        private final Map<String, InetSocketAddress> peers;
        private final int majority;
        private final TermStore store;
        private final RaftLog raftLog;
        private final ElectionTimer electionTimer = new ElectionTimer();
        private final ScheduledExecutorService clock = Executors.newSingleThreadScheduledExecutor();
        private final ExecutorService outbound;
        private final TcpClient client = new TcpClient();
        private final Set<String> votes = new HashSet<>();
        private final Set<String> inFlight = new HashSet<>();
        private final Map<String, PeerProgress> progress = new HashMap<>();
        private final Map<ClientKey, Long> appliedCommands = new LinkedHashMap<>();
        private final Map<Long, Long> proposedAt = new HashMap<>();
        private final Map<Long, Long> consensusNanos = new HashMap<>();
        private TermState state;
        private RaftRole role = RaftRole.FOLLOWER;
        private String leader;
        private long nextHeartbeat;
        private long lastApplied;
        private int clientWaiters;
        private boolean started;
        private boolean stopped;

        private record PeerProgress(long nextIndex, long matchIndex) {}
        private record ClientKey(String clientId, UUID requestId) {}

        public record Snapshot(RaftRole role, long term, String leader, long commitIndex, long lastApplied,
                               List<RaftLog.LogEntry> log, List<Long> appliedCommandIndexes,
                               Map<Long, Long> consensusNanos) {}

        public synchronized Snapshot snapshot() {
            return new Snapshot(role, state.term(), leader, state.commitIndex(), lastApplied, raftLog.snapshot(),
                    List.copyOf(appliedCommands.values()), Map.copyOf(consensusNanos));
        }

        public RaftNode(String id, Set<String> members, Map<String, InetSocketAddress> peers, Path dataDir)
                throws IOException {
            requireId(id);
            Set<String> expectedPeers = new HashSet<>(members);
            members.forEach(NodeMain::requireId);
            require(expectedPeers.remove(id), "La membresía debe incluir al nodo local");
            this.peers = Map.copyOf(peers);
            require(expectedPeers.equals(this.peers.keySet()), "Configura un endpoint por cada otra réplica");
            require(this.peers.values().stream().distinct().count() == this.peers.size(),
                    "Los endpoints de los peers deben ser distintos");
            this.id = id;
            majority = members.size() / 2 + 1;
            store = new TermStore(dataDir, id, Set.copyOf(members));
            state = store.initial;
            try {
                raftLog = new RaftLog(dataDir);
                if (raftLog.lastLogTerm() > state.term() || state.commitIndex() > raftLog.lastLogIndex()) {
                    var invalid = new IOException("El log y los metadatos Raft persistidos son incompatibles");
                    try {
                        raftLog.close();
                    } catch (IOException closeFailure) {
                        invalid.addSuppressed(closeFailure);
                    }
                    throw invalid;
                }
            } catch (IOException | RuntimeException failure) {
                try {
                    store.close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
            outbound = Executors.newFixedThreadPool(Math.max(1, this.peers.size()));
            applyCommitted(false);
        }

        public synchronized void start() {
            if (started || stopped) throw new IllegalStateException("Raft ya iniciado o cerrado");
            started = true;
            electionTimer.reset();
            log("startup");
            clock.scheduleWithFixedDelay(this::tick, 0, 25, TimeUnit.MILLISECONDS);
        }

        private synchronized void tick() {
            if (stopped) return;
            long now = System.nanoTime();
            try {
                if (role == RaftRole.LEADER) {
                    if (now - nextHeartbeat >= 0) replicateAll(now);
                } else if (electionTimer.expired(now)) {
                    beginElection();
                }
            } catch (IOException failure) {
                // La escritura fallida ya detuvo Raft. El listener puede seguir atendiendo DM1.
            }
        }

        private void beginElection() throws IOException {
            if (state.term() == Long.MAX_VALUE) {
                failStop("término agotado");
                return;
            }
            persist(new TermState(state.term() + 1, id, state.commitIndex()));
            votes.clear();
            votes.add(id);
            electionTimer.reset();
            transition(RaftRole.CANDIDATE, null, "election");
            if (votes.size() >= majority) {
                becomeLeader();
            } else {
                var request = new RequestVote(state.term(), raftLog.lastPosition());
                peers.keySet().forEach(peer -> submit(peer, request, RaftRole.CANDIDATE));
            }
        }

        private void becomeLeader() throws IOException {
            long firstNew = raftLog.lastLogIndex() + 1;
            propose(new NoOpEntry(state.term()));
            progress.clear();
            peers.keySet().forEach(peer -> progress.put(peer, new PeerProgress(firstNew, 0)));
            transition(RaftRole.LEADER, id, "majority");
            tryCommit();
            replicateAll(System.nanoTime());
        }

        private void replicateAll(long now) {
            nextHeartbeat = now + HEARTBEAT_INTERVAL;
            peers.keySet().forEach(this::replicate);
        }

        private void replicate(String peer) {
            if (inFlight.contains(peer)) return;
            AppendEntries request = raftLog.batch(state.term(), progress.get(peer).nextIndex(), id,
                    state.commitIndex());
            submit(peer, request, RaftRole.LEADER);
        }

        private void submit(String peer, Payload request, RaftRole expectedRole) {
            long term = state.term();
            if (inFlight.add(peer)) {
                outbound.execute(() -> send(peer, peers.get(peer), request, expectedRole, term));
            }
        }

        private void send(String peer, InetSocketAddress endpoint, Payload request, RaftRole expectedRole, long term) {
            Payload response = null;
            try {
                synchronized (this) {
                    if (!isCurrent(expectedRole, term)) return;
                }
                response = client.call(endpoint, peer, id, request).payload();
            } catch (IOException unavailable) {
                // La ausencia de respuesta no cuenta como voto ni reinicia plazos.
            } finally {
                synchronized (this) {
                    inFlight.remove(peer);
                    if (response != null && isCurrent(expectedRole, term)) {
                        try {
                            acceptResponse(peer, response);
                        } catch (IOException failure) {
                            // El fallo de persistencia ya detuvo el nodo Raft.
                        }
                    }
                }
            }
        }

        private boolean isCurrent(RaftRole expectedRole, long term) {
            return !stopped && role == expectedRole && state.term() == term;
        }

        private void acceptResponse(String peer, Payload response) throws IOException {
            if (response instanceof VoteResponse vote) {
                if (vote.term() > state.term()) observeHigherTerm(vote.term());
                else if (vote.voteGranted() && votes.add(peer) && votes.size() >= majority) becomeLeader();
            } else if (response instanceof AppendResponse append) {
                if (append.term() > state.term()) observeHigherTerm(append.term());
                else updateProgress(peer, append);
            }
        }

        private void updateProgress(String peer, AppendResponse response) throws IOException {
            PeerProgress prior = progress.get(peer);
            PeerProgress updated;
            if (response.success()) {
                long matched = Math.max(prior.matchIndex(), response.matchIndex());
                updated = new PeerProgress(matched + 1, matched);
            } else {
                long next = Math.max(prior.matchIndex() + 1, prior.nextIndex() - 1);
                updated = new PeerProgress(next, prior.matchIndex());
            }
            if (updated.equals(prior)) return;
            progress.put(peer, updated);
            System.out.printf("[RAFT %s] peer=%s term=%d nextIndex=%d matchIndex=%d event=%s%n",
                    id, peer, state.term(), updated.nextIndex(), updated.matchIndex(),
                    response.success() ? "replicated" : "replication-retry");
            if (response.success()) tryCommit();
            if (!response.success() || updated.nextIndex() <= raftLog.lastLogIndex()) replicate(peer);
        }

        private RaftLog.LogEntry propose(LogEntry entry) throws IOException {
            long start = System.nanoTime();
            try {
                RaftLog.LogEntry saved = raftLog.append(entry);
                proposedAt.put(saved.index(), start);
                return saved;
            } catch (IOException failure) {
                failStop(failure.toString());
                throw failure;
            }
        }

        private void tryCommit() throws IOException {
            long[] matched = new long[peers.size() + 1];
            matched[0] = raftLog.lastLogIndex();
            int i = 1;
            for (PeerProgress peer : progress.values()) matched[i++] = peer.matchIndex();
            Arrays.sort(matched);
            long quorumIndex = matched[matched.length - majority];
            if (quorumIndex > state.commitIndex() && raftLog.entryAt(quorumIndex).term() == state.term()) {
                advanceCommit(quorumIndex);
            }
        }

        private void advanceCommit(long index) throws IOException {
            if (index <= state.commitIndex()) return;
            persist(new TermState(state.term(), state.votedFor(), index));
            applyCommitted(true);
            notifyAll();
        }

        private void applyCommitted(boolean live) {
            long committedAt = System.nanoTime();
            while (lastApplied < state.commitIndex()) {
                RaftLog.LogEntry applied = raftLog.entryAt(++lastApplied);
                boolean effect = false;
                if (applied.entry() instanceof CommandEntry command) {
                    var key = new ClientKey(command.clientId(), command.clientRequestId());
                    effect = appliedCommands.putIfAbsent(key, applied.index()) == null;
                }
                Long start = proposedAt.remove(applied.index());
                Long elapsed = start == null ? null : committedAt - start;
                if (elapsed != null) consensusNanos.put(applied.index(), elapsed);
                if (live) {
                    System.out.printf("[RAFT %s] entryIndex=%d entryTerm=%d kind=%s commitIndex=%d lastApplied=%d "
                                    + "effect=%b consensusNanos=%s event=applied%n",
                            id, applied.index(), applied.term(), applied.type(), state.commitIndex(), lastApplied,
                            effect, elapsed == null ? "-" : elapsed);
                }
            }
        }

        static boolean upToDate(LogPosition candidate, LogPosition local) {
            return candidate.term() > local.term()
                    || (candidate.term() == local.term() && candidate.index() >= local.index());
        }

        @Override public synchronized VoteResponse requestVote(Header header, RequestVote vote) throws IOException {
            ensureAvailable();
            if (vote.term() > state.term()) observeHigherTerm(vote.term());
            String candidate = header.senderId();
            boolean granted = vote.term() == state.term()
                    && (state.votedFor() == null || state.votedFor().equals(candidate))
                    && upToDate(vote.lastLog(), raftLog.lastPosition());
            if (granted) {
                if (state.votedFor() == null) persist(new TermState(state.term(), candidate, state.commitIndex()));
                electionTimer.reset();
            }
            return new VoteResponse(state.term(), granted);
        }

        @Override public synchronized AppendResponse appendEntries(Header header, AppendEntries append)
                throws IOException {
            ensureAvailable();
            if (append.term() > state.term()) observeHigherTerm(append.term());
            if (append.term() < state.term() || (role == RaftRole.LEADER && !id.equals(header.senderId()))) {
                return new AppendResponse(state.term(), false, 0);
            }
            transition(RaftRole.FOLLOWER, header.senderId(), append.entries().isEmpty() ? "heartbeat" : "append");
            electionTimer.reset();
            try {
                boolean matches = raftLog.merge(append, state.commitIndex());
                if (matches) advanceCommit(Math.min(append.leaderCommit(), append.lastIndex()));
                return new AppendResponse(state.term(), matches, matches ? append.lastIndex() : 0);
            } catch (IOException failure) {
                failStop(failure.toString());
                throw failure;
            }
        }

        @Override public synchronized ClientResult clientCommand(Header header, ClientCommand command)
                throws IOException {
            ensureAvailable();
            if (role != RaftRole.LEADER) return new ClientResult(1, 0, leader);
            RaftLog.LogEntry prior = raftLog.findCommand(header.senderId(), header.requestId());
            if (prior != null && !((CommandEntry) prior.entry()).command().equals(command)) {
                return new ClientResult(3, 0, null);
            }
            if (prior == null) {
                prior = propose(new CommandEntry(state.term(), header.senderId(), header.requestId(), command));
                tryCommit();
            }
            if (prior.index() <= lastApplied) return new ClientResult(0, prior.index(), null);
            peers.keySet().forEach(this::replicate);
            return awaitCommit(prior.index(), state.term());
        }

        private ClientResult awaitCommit(long index, long term) throws IOException {
            if (clientWaiters >= MAX_CLIENT_WAITERS) return new ClientResult(2, 0, null);
            long deadline = System.nanoTime() + CLIENT_WAIT;
            clientWaiters++;
            try {
                while (isCurrent(RaftRole.LEADER, term)) {
                    if (index <= lastApplied) return new ClientResult(0, index, null);
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) break;
                    TimeUnit.NANOSECONDS.timedWait(this, remaining);
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException("Espera de commit interrumpida", interrupted);
            } finally {
                clientWaiters--;
            }
            ensureAvailable();
            if (role != RaftRole.LEADER) return new ClientResult(1, 0, leader);
            return new ClientResult(2, 0, null);
        }

        private void observeHigherTerm(long term) throws IOException {
            boolean wasLeader = role == RaftRole.LEADER;
            persist(new TermState(term, null, state.commitIndex()));
            votes.clear();
            if (wasLeader) electionTimer.reset();
            proposedAt.clear();
            role = RaftRole.FOLLOWER;
            leader = null;
            log("higher-term");
            notifyAll();
        }

        private void transition(RaftRole next, String knownLeader, String reason) {
            boolean changed = next != role || !Objects.equals(knownLeader, leader);
            if (role == RaftRole.LEADER && next != role) proposedAt.clear();
            role = next;
            leader = knownLeader;
            if (changed || next == RaftRole.CANDIDATE) log(reason);
            if (changed) notifyAll();
        }

        private void log(String reason) {
            System.out.printf("[RAFT %s] role=%s term=%d leader=%s event=%s commitIndex=%d lastApplied=%d lastLogIndex=%d%n",
                    id, role, state.term(), leader == null ? "-" : leader, reason,
                    state.commitIndex(), lastApplied, raftLog.lastLogIndex());
        }

        private void persist(TermState next) throws IOException {
            try {
                store.save(next);
                state = next;
            } catch (IOException failure) {
                failStop(failure.toString());
                throw failure;
            }
        }

        private void ensureAvailable() throws IOException {
            if (stopped) throw new IOException("Raft detenido");
        }

        private void failStop(String reason) {
            System.err.printf("[RAFT %s] stopped: %s%n", id, reason);
            stopExecutors();
        }

        private void stopExecutors() {
            stopped = true;
            notifyAll();
            clock.shutdownNow();
            outbound.shutdownNow();
        }

        private void shutdown() {
            try {
                close();
            } catch (IOException failure) {
                System.err.println("ERROR cerrando Raft: " + failure);
            }
        }

        @Override public synchronized void close() throws IOException {
            stopExecutors();
            try {
                raftLog.close();
            } finally {
                store.close();
            }
        }
    }

    private record TermState(long term, String votedFor, long commitIndex) {}

    /** Formato local. Se bloquea el directorio durante toda la ejecución del nodo. */
    private static final class TermStore implements AutoCloseable {
        private static final int LEGACY_FORMAT = 0x44525331; // DRS1
        private static final int FORMAT = 0x44525332; // DRS2
        private final Path directory;
        private final Path file;
        private final FileChannel lockChannel;
        private final String nodeId;
        private final Set<String> members;
        private final String membership;
        private final TermState initial;

        TermStore(Path directory, String nodeId, Set<String> members) throws IOException {
            this.directory = directory.toAbsolutePath();
            this.nodeId = nodeId;
            this.members = members;
            membership = String.join(",", members.stream().sorted().toList());
            file = this.directory.resolve("term-state.bin");
            Files.createDirectories(this.directory);
            lockChannel = FileChannel.open(this.directory.resolve("raft.lock"),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            try {
                FileLock lock = lockChannel.tryLock();
                if (lock == null) throw new IOException("Directorio Raft en uso: " + this.directory);
                initial = load();
            } catch (IOException | RuntimeException failure) {
                try {
                    close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }

        private TermState load() throws IOException {
            DataInputStream opened;
            try {
                opened = new DataInputStream(Files.newInputStream(file));
            } catch (NoSuchFileException missing) {
                TermState fresh = new TermState(0, null, 0);
                save(fresh);
                return fresh;
            }
            try (DataInputStream input = opened) {
                int format = input.readInt();
                if ((format != FORMAT && format != LEGACY_FORMAT) || !input.readUTF().equals(nodeId)
                        || !input.readUTF().equals(membership)) {
                    throw new IOException("Estado Raft incompatible con la identidad o membresía local");
                }
                long term = input.readLong();
                String vote = input.readUTF();
                long commitIndex = format == FORMAT ? input.readLong() : 0;
                if (term < 0 || commitIndex < 0 || (term == 0 && commitIndex != 0)
                        || (!vote.isEmpty() && (term == 0 || !members.contains(vote))) || input.read() != -1) {
                    throw new IOException("Estado Raft inválido");
                }
                return new TermState(term, vote.isEmpty() ? null : vote, commitIndex);
            }
        }

        private void save(TermState state) throws IOException {
            Path temporary = Files.createTempFile(directory, "term-state-", ".tmp");
            try {
                try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE);
                     DataOutputStream output = new DataOutputStream(Channels.newOutputStream(channel))) {
                    output.writeInt(FORMAT);
                    output.writeUTF(nodeId);
                    output.writeUTF(membership);
                    output.writeLong(state.term());
                    output.writeUTF(state.votedFor() == null ? "" : state.votedFor());
                    output.writeLong(state.commitIndex());
                    output.flush();
                    channel.force(true);
                }
                Files.move(temporary, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                syncDirectory(directory);
            } finally {
                Files.deleteIfExists(temporary);
            }
        }

        @Override public void close() throws IOException { lockChannel.close(); }
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
