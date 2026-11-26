package dogmessenger.media;

import java.io.*;
import dogmessenger.chat.Envelope;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public final class BlobServer {
    private static final int BLOCK = 8192, HEADER = 71, MAC = 32, LIMIT = BLOCK + 128;
    private static final int STATUS = 1, PUT = 2, SEAL = 3, GET = 4;
    private static final int MAGIC = 0x444d4231;
    private static final long MAX = 50L * 1024 * 1024;
    private static final HexFormat HEX = HexFormat.of();
    private final Path root;
    private final String node;
    private final byte[] key, receipts;
    private final ReentrantLock[] locks = new ReentrantLock[64];

    private BlobServer(Path root, String node, byte[] key, byte[] receipts) throws IOException {
        boolean existed = Files.exists(root);
        this.root = Files.createDirectories(root.toAbsolutePath());
        if (!existed) syncDirectory(this.root.getParent());
        this.node = node;
        this.key = key;
        this.receipts = receipts;
        Arrays.setAll(locks, i -> new ReentrantLock());
    }

    public static byte[] sign(byte[] key, byte[] body) throws GeneralSecurityException {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        return mac.doFinal(body);
    }

    private static byte[] secret(String variable) {
        String hex = System.getenv().getOrDefault(variable, "");
        check(hex.matches("[0-9a-f]{64}"), variable + " debe ser hexadecimal de 32 bytes");
        return HEX.parseHex(hex);
    }

    public static byte[] proof(byte[] key, String node, byte[] digest, long size)
            throws GeneralSecurityException {
        byte[] domain = "DMB1-durable\0".getBytes(StandardCharsets.US_ASCII);
        var statement = ByteBuffer.allocate(domain.length + 2 + 32 + 8)
                .put(domain).put(node.getBytes(StandardCharsets.US_ASCII)).put(digest).putLong(size);
        return sign(key, statement.array());
    }

    public static boolean admitsAttachment(byte[] data, Set<String> members, UUID request, String client) {
        if (data.length < 6 || data[0] != 'C' || data[1] != 'H' || data[2] != 'A' || data[3] != 'T'
            || data[4] != '1' || (data[5] != 10 && data[5] != 15)) return true;
        try {
            var operation = Envelope.decode(data, secret("DOG_CHAT_KEY"));
            var fields = operation.fields();
            if (!client.equals("chat-app") || !operation.requestId().equals(request)
                || !request.toString().replace("-", "").equals(fields.get(2))) return false;
            long size = Long.parseLong(fields.get(6));
            byte[] digest = HEX.parseHex(fields.get(7));
            if (size < 1 || size > MAX || digest.length != 32) return false;
            byte[] key = secret("DOG_MEDIA_RECEIPT_KEY");
            var observed = new HashSet<String>();
            for (String row : fields.get(8).split(",", -1)) {
                String[] pair = row.split(":", -1);
                if (pair.length != 2 || !members.contains(pair[0]) || !observed.add(pair[0])) return false;
                if (!MessageDigest.isEqual(proof(key, pair[0], digest, size), HEX.parseHex(pair[1]))) return false;
            }
            return observed.equals(members);
        } catch (IOException | GeneralSecurityException | IllegalArgumentException error) {
            return false;
        }
    }

    private static void check(boolean ok, String reason) {
        if (!ok) throw new IllegalArgumentException(reason);
    }

    private static void syncDirectory(Path directory) throws IOException {
        if (System.getProperty("os.name").startsWith("Windows")) return;
        try (var channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private String digest(Path path) throws IOException, GeneralSecurityException {
        var hash = MessageDigest.getInstance("SHA-256");
        try (var input = Files.newInputStream(path)) {
            byte[] chunk = new byte[65536];
            for (int n; (n = input.read(chunk)) >= 0;) hash.update(chunk, 0, n);
        }
        return HEX.formatHex(hash.digest());
    }

    private byte[] apply(int op, String hash, long size, long offset, byte[] data)
            throws IOException, GeneralSecurityException {
        check(size > 0 && size <= MAX && offset >= 0 && offset <= size, "INVALID_BLOCK");
        Path part = root.resolve(hash + ".part");
        Path complete = root.resolve(hash + ".bin");
        var lock = locks[Integer.parseInt(hash.substring(0, 2), 16) % locks.length];
        lock.lock();
        try {
            int count = (int)Math.min(BLOCK, size - offset);
            if (op == GET) {
                check(data.length == 0 && offset < size && offset % BLOCK == 0
                      && Files.isRegularFile(complete) && Files.size(complete) == size, "MISSING_BLOB");
                try (var file = new RandomAccessFile(complete.toFile(), "r")) {
                    byte[] result = new byte[count];
                    file.seek(offset);
                    file.readFully(result);
                    return result;
                }
            }
            long current = Files.exists(part) ? Files.size(part) : 0;
            if (current > 0 && current < size && current % BLOCK != 0) {
                current -= current % BLOCK;
                try (var file = new RandomAccessFile(part.toFile(), "rw")) {
                    file.setLength(current);
                    file.getFD().sync();
                }
            }
            if (op == STATUS || op == SEAL) {
                check(data.length == 0 && offset == 0, "INVALID_BLOCK");
                if (Files.isRegularFile(complete) && Files.size(complete) == size && digest(complete).equals(hash)) {
                    syncDirectory(root);
                    return ByteBuffer.allocate(9).put((byte)1).putLong(size).array();
                }
                check(current <= size && (current == size || current % BLOCK == 0), "INVALID_PART");
                if (op == STATUS) return ByteBuffer.allocate(9).put((byte)0).putLong(current).array();
                check(current == size, "INCOMPLETE_BLOB");
                if (!digest(part).equals(hash)) {
                    Files.delete(part);
                    syncDirectory(root);
                    throw new IllegalArgumentException("HASH_MISMATCH");
                }
                Files.move(part, complete, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                syncDirectory(root);
                return ByteBuffer.allocate(9).put((byte)1).putLong(size).array();
            }
            check(op == PUT && offset < size && offset % BLOCK == 0 && data.length == count, "INVALID_BLOCK");
            if (offset < current) {
                try (var file = new RandomAccessFile(part.toFile(), "r")) {
                    byte[] existing = new byte[count];
                    file.seek(offset);
                    file.readFully(existing);
                    check(Arrays.equals(existing, data), "BLOCK_CONFLICT");
                }
                return ByteBuffer.allocate(8).putLong(current).array();
            }
            check(offset == current, "OUT_OF_ORDER");
            check(Files.getFileStore(root).getUsableSpace() >= count + BLOCK, "NO_SPACE");
            try (var file = new RandomAccessFile(part.toFile(), "rw")) {
                file.seek(offset);
                try {
                    file.write(data);
                    file.getFD().sync();
                } catch (IOException error) {
                    file.setLength(current);
                    throw error;
                }
            }
            syncDirectory(root);
            return ByteBuffer.allocate(8).putLong(current + count).array();
        } finally {
            lock.unlock();
        }
    }

    private static byte[] receive(Socket conn, int size, long deadline) throws IOException {
        var input = conn.getInputStream();
        byte[] raw = new byte[size];
        for (int offset = 0; offset < size;) {
            long ms = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (ms <= 0) throw new SocketTimeoutException();
            conn.setSoTimeout((int)ms);
            int count = input.read(raw, offset, size - offset);
            if (count == -1) throw new EOFException();
            offset += count;
        }
        return raw;
    }

    private void handle(Socket conn) {
        try (conn) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            int length = ByteBuffer.wrap(receive(conn, 4, deadline)).getInt();
            if (length < HEADER + MAC || length > LIMIT) return;
            byte[] raw = receive(conn, length, deadline);
            byte[] body = Arrays.copyOf(raw, length - MAC);
            if (!MessageDigest.isEqual(sign(key, body), Arrays.copyOfRange(raw, length - MAC, length))) return;
            var parser = ByteBuffer.wrap(body);
            if (parser.getInt() != MAGIC) return;
            int op = Byte.toUnsignedInt(parser.get());
            var request = new UUID(parser.getLong(), parser.getLong());
            byte[] target = new byte[2];
            parser.get(target);
            if (!node.equals(new String(target, StandardCharsets.US_ASCII)) ||
                request.equals(new UUID(0, 0)) || op < STATUS || op > GET) return;
            byte[] hash = new byte[32];
            parser.get(hash);
            long size = parser.getLong();
            long offset = parser.getLong();
            byte[] data = new byte[parser.remaining()];
            parser.get(data);
            byte[] result;
            byte status = 0;
            try {
                result = apply(op, HEX.formatHex(hash), size, offset, data);
                if ((op == STATUS || op == SEAL) && result[0] == 1) {
                    result = ByteBuffer.allocate(41).put(result).put(proof(receipts, node, hash, size)).array();
                }
            } catch (IllegalArgumentException | IOException error) {
                status = 1;
                String reason = error instanceof IOException ? "STORAGE_ERROR" : error.getMessage();
                result = reason.getBytes(StandardCharsets.US_ASCII);
            }
            byte[] response = ByteBuffer.allocate(HEADER + 1 + result.length)
                    .put(body, 0, HEADER).put(status).put(result).array();
            response[4] |= (byte)128;
            var output = new DataOutputStream(conn.getOutputStream());
            output.writeInt(response.length + MAC);
            output.write(response);
            output.write(sign(key, response));
        } catch (IOException | GeneralSecurityException ignored) {
            // El cierre señala una conexión incompleta o no autenticada.
        }
    }

    public static void main(String[] args) throws Exception {
        var options = new HashMap<String, String>();
        if (args.length % 2 != 0) throw new IllegalArgumentException("Argumentos DMB1 inválidos");
        for (int i = 0; i < args.length; i += 2) {
            check(Set.of("--node", "--host", "--port", "--data-dir").contains(args[i])
                  && options.putIfAbsent(args[i], args[i + 1]) == null, "Argumentos DMB1 inválidos");
        }
        String node = options.getOrDefault("--node", "");
        check(node.matches("n[123]"), "Nodo DMB1 inválido");
        byte[] key = secret("DOG_MEDIA_KEY"), receipts = secret("DOG_MEDIA_RECEIPT_KEY");
        check(!MessageDigest.isEqual(key, receipts), "Las claves DMB1 y de recibos deben ser distintas");
        var storage = new BlobServer(Path.of(options.get("--data-dir")), node, key, receipts);
        var executor = new ThreadPoolExecutor(6, 6, 0, TimeUnit.SECONDS, new SynchronousQueue<>());
        try (var guard = FileChannel.open(storage.root.resolve(".dmb-lock"), StandardOpenOption.CREATE,
                                          StandardOpenOption.WRITE);
             var lock = guard.tryLock();
             var server = new ServerSocket()) {
            if (lock == null) throw new IOException("Almacén DMB1 en uso");
            server.setReuseAddress(true);
            String host = options.getOrDefault("--host", "127.0.0.1");
            int port = Integer.parseInt(options.get("--port"));
            server.bind(new InetSocketAddress(host, port), 6);
            System.out.printf("DMB1 %s %s:%d%n", node, host, server.getLocalPort());
            while (true) {
                Socket client = server.accept();
                try {
                    executor.execute(() -> storage.handle(client));
                } catch (RejectedExecutionException rejected) {
                    client.close();
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
