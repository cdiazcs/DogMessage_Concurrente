package dogmessenger.chat;

import dogmessenger.twopc.Wire;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** Exportación optativa de operaciones aplicadas en loopback autenticado. */
public final class Feed {
    public static final int ALGORITHM = 3;
    public static final byte[] MAGIC = "CHAT1".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] DOMAIN = "DM2-chat-feed-v1".getBytes(StandardCharsets.US_ASCII);
    private static final int LIMIT = 64;

    public record Entry(long position, byte[] payload) {}
    public record Snapshot(long cursor, List<Entry> entries) {}
    @FunctionalInterface public interface Source { Snapshot snapshot(long after) throws IOException; }

    private Feed() {}

    public static byte[] key() {
        String value = System.getenv("DOG_CHAT_KEY");
        if (value == null || !value.matches("[0-9a-fA-F]{64}")) {
            throw new IllegalArgumentException("--chat-feed requiere DOG_CHAT_KEY de 32 bytes hex");
        }
        return HexFormat.of().parseHex(value);
    }

    public static boolean isFeed(byte[] body) {
        return body.length >= 5 && ByteBuffer.wrap(body).getInt() == 0x444d3200 && body[4] == ALGORITHM;
    }

    public static boolean isChat(byte[] body) {
        return body.length >= MAGIC.length && Arrays.equals(body, 0, MAGIC.length, MAGIC, 0, MAGIC.length);
    }

    public static String diagnosticBytes(byte[] body) {
        return isChat(body) ? "REDACTED_CHAT" : HexFormat.of().formatHex(body);
    }

    private static byte[] signature(byte[] key, byte[] data) throws IOException {
        try {
            var mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            mac.update(DOMAIN);
            return mac.doFinal(data);
        } catch (java.security.GeneralSecurityException error) {
            throw new IOException("HMAC no disponible", error);
        }
    }

    public static byte[] dispatch(byte[] body, String id, byte[] key, Source source) throws IOException {
        Wire.Frame frame;
        long after;
        try {
            frame = Wire.header(body);
            if (frame.header().algorithm() != ALGORITHM || frame.header().kind() != 1) return null;
            after = Wire.sequence(frame.input(), true);
            var sig = frame.input().readNBytes(32);
            if (sig.length != 32 || frame.input().read() != -1) return null;
            var token = frame.header().token();
            byte[] challenge = ByteBuffer.allocate(24).putLong(token.getMostSignificantBits())
                    .putLong(token.getLeastSignificantBits()).putLong(after).array();
            if (!MessageDigest.isEqual(sig, signature(key, challenge))) return null;
        } catch (IOException malformed) {
            return null;
        }
        var snapshot = source.snapshot(after);
        var selected = new ArrayList<Entry>();
        int size = 23 + id.length() + 10;
        long cursor = snapshot.cursor();
        for (var entry : snapshot.entries()) {
            if (entry.position() <= after || !isChat(entry.payload())) continue;
            if (selected.size() == LIMIT || size + 12 + entry.payload().length > Wire.MAX_BODY) {
                cursor = selected.get(selected.size() - 1).position();
                break;
            }
            selected.add(entry);
            size += 12 + entry.payload().length;
        }
        long next = cursor;
        return Wire.encode(new Wire.Header(ALGORITHM, 0x81, frame.header().token(), id), out -> {
            out.writeLong(next);
            out.writeShort(selected.size());
            for (var entry : selected) {
                out.writeLong(entry.position());
                out.writeInt(entry.payload().length);
                out.write(entry.payload());
            }
        });
    }
}
