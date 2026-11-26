package dogmessenger.twopc;

import static dogmessenger.twopc.Wire.*;

import dogmessenger.chat.Feed;
import dogmessenger.media.BlobServer;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.CRC32C;

/** Motor JVM 2PC. Java y Kotlin comparten la política y la persistencia. */
public final class Engine implements AutoCloseable {
    private static final int UNKNOWN = 0, YES = 1, ABORTED = 2;
    private final String id, coordinator;
    private final Map<String, InetSocketAddress> peers;
    private final Store store;
    private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor();
    private boolean stopped;

    private record Row(Tx tx, int decision, Map<String, Integer> votes, Set<String> acks) {
        Row(Tx tx, int decision) { this(tx, decision, Map.of(), Set.of()); }
        Row decide(int value) { return new Row(tx, value, votes, acks); }
        Row vote(String peer, int value) {
            var copy = new HashMap<>(votes);
            copy.put(peer, value);
            return new Row(tx, decision, Map.copyOf(copy), acks);
        }
        Row ack(String peer) {
            var copy = new HashSet<>(acks);
            copy.add(peer);
            return new Row(tx, decision, votes, Set.copyOf(copy));
        }
    }

    public Engine(String id, String coordinator, Map<String, InetSocketAddress> peers,
                  Path directory, boolean inspect) throws IOException {
        require(validId(id) && validId(coordinator) && !peers.containsKey(id)
                && (id.equals(coordinator) || peers.containsKey(coordinator)));
        require(peers.keySet().stream().allMatch(Wire::validId)
                && peers.values().stream().distinct().count() == peers.size());
        this.id = id;
        this.coordinator = coordinator;
        this.peers = Map.copyOf(peers);
        store = new Store(directory);
        try {
            if (!inspect && isCoordinator() && active() != null && active().decision() == UNKNOWN) {
                update(active().decide(ABORTED));
            }
        } catch (IOException | RuntimeException error) {
            store.close();
            throw error;
        }
    }

    public void start() {
        worker.scheduleWithFixedDelay(this::cycle, 0, 75, TimeUnit.MILLISECONDS);
    }

    private boolean isCoordinator() { return id.equals(coordinator); }
    private Row active() { return store.rows.isEmpty() ? null : store.rows.get(store.rows.size() - 1); }

    private void update(Row row) throws IOException {
        var next = new ArrayList<>(store.rows);
        if (row.tx().sequence() == next.size() + 1L) next.add(row);
        else {
            require(row.tx().sequence() == next.size());
            next.set(next.size() - 1, row);
        }
        try {
            store.save(List.copyOf(next));
        } catch (IOException error) {
            stopped = true;
            System.err.printf("[2PC %s] ERROR persistencia: %s%n", id, error);
            throw error;
        }
    }

    private void cycle() {
        try {
            tick();
        } catch (IOException error) {
            synchronized (this) { stopped = true; }
            System.err.printf("[2PC %s] ERROR %s%n", id, error);
        }
    }

    private void tick() throws IOException {
        Row row;
        synchronized (this) {
            if (stopped) return;
            row = active();
        }
        if (row == null) return;
        if (!isCoordinator()) {
            if (row.decision() == UNKNOWN) {
                int decision = call(coordinator, QUERY, row.tx());
                if (decision == YES || decision == ABORTED) {
                    synchronized (this) {
                        if (!stopped) participant(decision == YES ? COMMIT : ABORT, row.tx());
                    }
                }
            }
            return;
        }
        if (row.decision() == UNKNOWN) {
            for (String peer : new TreeSet<>(peers.keySet())) {
                int vote = call(peer, PREPARE, row.tx());
                synchronized (this) {
                    if (stopped) return;
                    if (vote >= 0) {
                        row = row.vote(peer, vote);
                        update(row);
                    }
                }
                if (vote != YES) break;
            }
            synchronized (this) {
                if (stopped) return;
                boolean allYes = row.votes().keySet().equals(peers.keySet()) &&
                        row.votes().values().stream().allMatch(value -> value == YES);
                row = row.decide(allYes ? YES : ABORTED);
                update(row);
                System.out.printf("[2PC %s] decision=%d sequence=%d%n", id, row.decision(), row.tx().sequence());
            }
        }
        for (String peer : new TreeSet<>(peers.keySet())) {
            if (row.acks().contains(peer)) continue;
            synchronized (this) { if (stopped) return; }
            if (call(peer, row.decision() == YES ? COMMIT : ABORT, row.tx()) == YES) {
                synchronized (this) {
                    if (stopped) return;
                    row = row.ack(peer);
                    update(row);
                }
            }
        }
    }

    private int call(String peer, int kind, Tx tx) {
        var header = new Header(ALGORITHM, kind, UUID.randomUUID(), id);
        try {
            byte[] outgoing = encode(header, out -> writePayload(out, kind == QUERY ? reference(tx) : tx));
            return correlate(exchange(peers.get(peer), outgoing), header, peer, reference(tx)).status();
        } catch (IOException error) {
            return -1;
        }
    }

    private static byte[] exchange(InetSocketAddress endpoint, byte[] body) throws IOException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(700);
        try (var channel = SocketChannel.open(); var selector = Selector.open()) {
            channel.configureBlocking(false);
            var key = channel.register(selector, 0);
            if (!channel.connect(endpoint)) {
                while (!channel.finishConnect()) ready(selector, key, SelectionKey.OP_CONNECT, deadline);
            }
            var outgoing = ByteBuffer.allocate(4 + body.length).putInt(body.length).put(body).flip();
            while (outgoing.hasRemaining()) {
                remaining(deadline);
                if (channel.write(outgoing) == 0) ready(selector, key, SelectionKey.OP_WRITE, deadline);
            }
            var header = ByteBuffer.allocate(4);
            read(channel, selector, key, header, deadline);
            int size = header.flip().getInt();
            require(size > 0 && size <= MAX_BODY);
            var input = ByteBuffer.allocate(size);
            read(channel, selector, key, input, deadline);
            return input.array();
        }
    }

    private static void read(SocketChannel channel, Selector selector, SelectionKey key, ByteBuffer buffer,
                             long deadline) throws IOException {
        while (buffer.hasRemaining()) {
            remaining(deadline);
            int count = channel.read(buffer);
            if (count < 0) throw new EOFException("Trama 2PC incompleta");
            if (count == 0) ready(selector, key, SelectionKey.OP_READ, deadline);
        }
    }

    private static long remaining(long deadline) throws IOException {
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0 || Thread.currentThread().isInterrupted()) throw new IOException("Timeout 2PC");
        return TimeUnit.NANOSECONDS.toMillis(nanos) + 1;
    }

    private static void ready(Selector selector, SelectionKey key, int op, long deadline) throws IOException {
        key.interestOps(op);
        do { selector.selectedKeys().clear(); } while (selector.select(remaining(deadline)) == 0);
    }

    private Result submit(Header request, byte[] bytes) throws IOException {
        if (!isCoordinator()) return new Result(4, 0, coordinator);
        for (Row row : store.rows) {
            Tx tx = row.tx();
            if (tx.client().equals(request.sender()) && tx.request().equals(request.token())) {
                if (!Arrays.equals(bytes, tx.bytes())) return new Result(3, 0, coordinator);
                if (row.decision() != UNKNOWN && row.acks().equals(peers.keySet())) {
                    return new Result(row.decision() == YES ? 0 : 1, tx.sequence(), coordinator);
                }
                return new Result(2, tx.sequence(), coordinator);
            }
        }
        Row latest = active();
        if (latest != null && (latest.decision() == UNKNOWN || !latest.acks().equals(peers.keySet()))) {
            return new Result(2, 0, coordinator);
        }
        var members = new HashSet<>(peers.keySet());
        members.add(id);
        if (!BlobServer.admitsAttachment(bytes, members, request.token(), request.sender())) {
            return new Result(3, 0, coordinator);
        }
        var tx = new Tx(store.rows.size() + 1L, UUID.randomUUID(), coordinator, request.sender(), request.token(), bytes);
        update(new Row(tx, UNKNOWN));
        return new Result(2, tx.sequence(), coordinator);
    }

    private int participant(int kind, Tx tx) throws IOException {
        if (tx.sequence() <= store.rows.size()) {
            Row row = store.rows.get((int) tx.sequence() - 1);
            if (!row.tx().matches(tx)) return 0;
            if (kind == PREPARE) return row.decision() == ABORTED ? 0 : 1;
            int decision = kind == COMMIT ? YES : ABORTED;
            if (row.decision() != UNKNOWN) return row.decision() == decision ? 1 : 0;
            update(row.decide(decision));
            return 1;
        }
        if (tx.sequence() != store.rows.size() + 1L || (active() != null && active().decision() == UNKNOWN)
                || kind == COMMIT) return 0;
        for (Row row : store.rows) {
            Tx old = row.tx();
            if (old.id().equals(tx.id()) || (old.client().equals(tx.client()) && old.request().equals(tx.request()))) {
                return 0;
            }
        }
        update(new Row(tx, kind == ABORT ? ABORTED : UNKNOWN));
        return 1;
    }

    public synchronized byte[] dispatch(byte[] body) throws IOException {
        Frame frame;
        try {
            frame = header(body);
        } catch (IOException invalid) {
            return null;
        }
        Header request = frame.header();
        int kind = request.kind();
        if (kind == 0 || kind >= 0x80) return null;
        if (request.algorithm() != ALGORITHM) return reply(request, 0xff, 1);
        if (kind > QUERY) return reply(request, 0xff, 2);
        Object data;
        try {
            data = readPayload(kind, frame.input());
        } catch (IOException invalid) {
            return reply(request, 0xff, 3);
        }
        if (kind != SUBMIT && !peers.containsKey(request.sender())) return reply(request, 0xff, 4);
        if (stopped) return null;
        if (kind == SUBMIT) return reply(request, 0x81, submit(request, (byte[]) data));
        if (kind == QUERY) {
            var ref = (Reference) data;
            int decision = UNKNOWN;
            if (isCoordinator() && ref.sequence() <= store.rows.size()) {
                Row row = store.rows.get((int) ref.sequence() - 1);
                if (row.tx().id().equals(ref.id())) decision = row.decision();
            }
            return reply(request, 0x85, new Reply(ref, decision));
        }
        var tx = (Tx) data;
        int accepted = !isCoordinator() && coordinator.equals(request.sender()) && coordinator.equals(tx.coordinator())
                ? participant(kind, tx) : 0;
        return reply(request, kind | 0x80, new Reply(reference(tx), accepted));
    }

    private byte[] reply(Header request, int kind, Object payload) throws IOException {
        return encode(new Header(request.algorithm(), kind, request.token(), id), out -> writePayload(out, payload));
    }

    public synchronized Feed.Snapshot chatSnapshot(long after) {
        int cursor = store.rows.size();
        if (active() != null && active().decision() == UNKNOWN) cursor--;
        var entries = new ArrayList<Feed.Entry>();
        for (int i = (int) Math.min(after, cursor); i < cursor; i++) {
            Row row = store.rows.get(i);
            if (row.decision() == YES) entries.add(new Feed.Entry(row.tx().sequence(), row.tx().bytes()));
        }
        return new Feed.Snapshot(cursor, entries);
    }

    public synchronized void inspect() {
        for (Row row : store.rows) {
            Tx tx = row.tx();
            String status = switch (row.decision()) {
                case YES -> "COMMIT";
                case ABORTED -> "ABORT";
                default -> isCoordinator() ? "STARTED" : "PREPARED";
            };
            System.out.printf("sequence=%d status=%s tx=%s client=%s requestId=%s command=%s%n",
                    tx.sequence(), status, tx.id(), tx.client(), tx.request(), Feed.diagnosticBytes(tx.bytes()));
        }
    }

    @Override public synchronized void close() throws IOException {
        stopped = true;
        worker.shutdownNow();
        store.close();
    }

    private final class Store implements AutoCloseable {
        private final Path directory, file;
        private final FileChannel lock;
        private List<Row> rows;

        Store(Path path) throws IOException {
            directory = path;
            Files.createDirectories(path);
            for (String name : List.of("raft_state.json", "raft-state.bin", "term-state.bin", "raft.lock",
                    "kotlin-raft.lock", "two-pc-python.json")) {
                if (Files.exists(path.resolve(name))) throw new IOException("2PC exige un directorio separado para este motor");
            }
            file = path.resolve("two-pc-jvm.bin");
            lock = FileChannel.open(path.resolve("twopc.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            try {
                if (lock.tryLock() == null) throw new IOException("Directorio 2PC en uso");
                rows = load();
            } catch (IOException | RuntimeException error) {
                lock.close();
                throw error;
            }
        }

        String membership() { return String.join(",", new TreeSet<>(peers.keySet())); }

        List<Row> load() throws IOException {
            byte[] all;
            try {
                all = Files.readAllBytes(file);
            } catch (NoSuchFileException fresh) {
                return List.of();
            }
            require(all.length >= 4);
            var crc = new CRC32C();
            crc.update(all, 0, all.length - 4);
            require((int) crc.getValue() == ByteBuffer.wrap(all, all.length - 4, 4).getInt());
            var input = new DataInputStream(new ByteArrayInputStream(all, 0, all.length - 4));
            require(input.readInt() == 0x54504331 && input.readUTF().equals(id)
                    && input.readUTF().equals(coordinator) && input.readUTF().equals(membership()));
            int count = input.readInt();
            require(count >= 0 && count <= all.length / 45);
            var loaded = new ArrayList<Row>();
            var identifiers = new HashSet<UUID>();
            var requests = new HashSet<Map.Entry<String, UUID>>();
            for (int i = 0; i < count; i++) {
                Tx tx = transaction(input);
                int decision = input.readUnsignedByte();
                require(tx.sequence() == i + 1L && decision <= 2 && tx.coordinator().equals(coordinator));
                require(identifiers.add(tx.id()) && requests.add(Map.entry(tx.client(), tx.request())));
                var votes = new HashMap<String, Integer>();
                int voteCount = input.readInt();
                require(voteCount >= 0 && voteCount <= peers.size());
                for (int j = 0; j < voteCount; j++) {
                    String peer = node(input);
                    int vote = input.readUnsignedByte();
                    require(peers.containsKey(peer) && vote <= 1 && votes.putIfAbsent(peer, vote) == null);
                }
                var acks = new HashSet<String>();
                int ackCount = input.readInt();
                require(ackCount >= 0 && ackCount <= peers.size());
                for (int j = 0; j < ackCount; j++) {
                    String peer = node(input);
                    require(peers.containsKey(peer) && acks.add(peer));
                }
                require(decision != UNKNOWN || (i == count - 1 && acks.isEmpty()));
                if (isCoordinator()) {
                    require(i == count - 1 || acks.equals(peers.keySet()));
                    if (decision == YES) require(votes.keySet().equals(peers.keySet())
                            && votes.values().stream().allMatch(vote -> vote == YES));
                } else require(votes.isEmpty() && acks.isEmpty());
                loaded.add(new Row(tx, decision, Map.copyOf(votes), Set.copyOf(acks)));
            }
            require(input.read() == -1);
            return List.copyOf(loaded);
        }

        void save(List<Row> next) throws IOException {
            Path temporary = Files.createTempFile(directory, "twopc-", ".tmp");
            try {
                try (var channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                    var raw = Channels.newOutputStream(channel);
                    var checksum = new java.util.zip.CheckedOutputStream(raw, new CRC32C());
                    var output = new DataOutputStream(checksum);
                    output.writeInt(0x54504331);
                    output.writeUTF(id);
                    output.writeUTF(coordinator);
                    output.writeUTF(membership());
                    output.writeInt(next.size());
                    for (Row row : next) {
                        transaction(output, row.tx());
                        output.writeByte(row.decision());
                        output.writeInt(row.votes().size());
                        for (var vote : new TreeMap<>(row.votes()).entrySet()) {
                            node(output, vote.getKey());
                            output.writeByte(vote.getValue());
                        }
                        output.writeInt(row.acks().size());
                        for (String peer : new TreeSet<>(row.acks())) node(output, peer);
                    }
                    new DataOutputStream(raw).writeInt((int) checksum.getChecksum().getValue());
                    channel.force(true);
                }
                Files.move(temporary, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                if (File.separatorChar == '/') {
                    try (var parent = FileChannel.open(directory, StandardOpenOption.READ)) { parent.force(true); }
                }
                rows = next;
            } finally {
                Files.deleteIfExists(temporary);
            }
        }

        @Override public void close() throws IOException { lock.close(); }
    }
}
