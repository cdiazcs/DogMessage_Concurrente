package dogmessenger.chat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.*;
import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

/** Codec JVM de la versión de aplicación, independiente del codec de consenso. */
public final class Envelope {
    private static final int[] FIELDS = {0, 6, 2, 1, 2, 4, 3, 3, 4, 1, 9, 4, 2, 2, 6, 12, 7, 4, 2, 5};
    public static final int ITERATIONS = 600_000;

    public record Operation(int kind, UUID requestId, List<String> fields) {}

    private Envelope() {}

    public static byte[] randomSalt() {
        byte[] bytes = new byte[16];
        new SecureRandom().nextBytes(bytes);
        return bytes;
    }

    public static byte[] derive(char[] password, byte[] salt) throws GeneralSecurityException {
        var spec = new PBEKeySpec(password, salt, ITERATIONS, 256);
        try {
            return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded();
        } finally {
            spec.clearPassword();
        }
    }

    private static byte[] sign(byte[] payload, byte[] key) throws GeneralSecurityException {
        var mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        return mac.doFinal(payload);
    }

    private static void require(boolean valid) throws IOException {
        if (!valid) throw new IOException("Sobre CHAT1 inválido");
    }

    public static Operation decode(byte[] raw, byte[] key) throws IOException, GeneralSecurityException {
        require(Feed.isChat(raw) && raw.length >= 54 && raw.length <= 65_536);
        byte[] payload = Arrays.copyOf(raw, raw.length - 32);
        require(MessageDigest.isEqual(Arrays.copyOfRange(raw, payload.length, raw.length), sign(payload, key)));
        var input = ByteBuffer.wrap(payload).position(5);
        int kind = Byte.toUnsignedInt(input.get());
        require(kind > 0 && kind < FIELDS.length);
        var id = new UUID(input.getLong(), input.getLong());
        require(!id.equals(new UUID(0, 0)));
        var fields = new ArrayList<String>(FIELDS[kind]);
        for (int i = 0; i < FIELDS[kind]; i++) {
            require(input.remaining() >= 2);
            int length = Short.toUnsignedInt(input.getShort());
            require(length <= 8192 && input.remaining() >= length);
            byte[] text = new byte[length];
            input.get(text);
            fields.add(StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(text)).toString());
        }
        require(!input.hasRemaining());
        return new Operation(kind, id, List.copyOf(fields));
    }

    public static byte[] encode(Operation operation, byte[] key) throws IOException, GeneralSecurityException {
        int kind = operation.kind();
        require(kind > 0 && kind < FIELDS.length && operation.fields().size() == FIELDS[kind]);
        require(!operation.requestId().equals(new UUID(0, 0)));
        var fields = new ArrayList<byte[]>();
        int size = 22;
        for (String text : operation.fields()) {
            var utf8 = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).encode(java.nio.CharBuffer.wrap(text));
            byte[] bytes = new byte[utf8.remaining()];
            utf8.get(bytes);
            require(bytes.length <= 8192);
            size += 2 + bytes.length;
            fields.add(bytes);
        }
        require(size <= 65_536 - 32);
        var result = ByteBuffer.allocate(size + 32).put(Feed.MAGIC).put((byte) kind)
                .putLong(operation.requestId().getMostSignificantBits())
                .putLong(operation.requestId().getLeastSignificantBits());
        for (byte[] field : fields) result.putShort((short) field.length).put(field);
        result.put(sign(Arrays.copyOf(result.array(), size), key));
        return result.array();
    }
}
