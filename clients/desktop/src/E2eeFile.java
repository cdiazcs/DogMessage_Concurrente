import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.HexFormat;
import javax.crypto.Cipher;

/** DGF1 trabaja sobre bloques, tanto al cifrar como al verificar. */
final class E2eeFile {
    private static final int BLOCK = 8192, HEADER = 20, MAGIC = 0x44474631;
    private static final long LIMIT = 50L * 1024 * 1024;
    record Sealed(Path path, String key, long size, String sha256) {}

    private static long limit(long size) throws IOException {
        if (size <= 0 || size > LIMIT) throw new IOException("Tamaño de archivo inválido");
        long encrypted = HEADER + size + 16 * ((size + BLOCK - 1) / BLOCK);
        if (encrypted > LIMIT) throw new IOException("El archivo cifrado supera 50 MiB");
        return encrypted;
    }

    private static String aad(String conversation, String message, long size, int index) throws IOException {
        if (!conversation.matches("[0-9a-f]{32}") || !message.matches("[0-9a-f]{32}")) {
            throw new IOException("ID DGF1 inválido");
        }
        return String.join("\n", "dog.file.1", conversation, message, Long.toString(size), Integer.toString(index));
    }

    private static byte[] nonce(byte[] prefix, int index) {
        return ByteBuffer.allocate(12).put(prefix).putInt(index).array();
    }

    static Sealed encrypt(Path source, String conversation, String message) throws Exception {
        long size = Files.size(source);
        long expected = limit(size);
        byte[] key = E2ee.random(32), prefix = E2ee.random(8);
        Path sealed = Files.createTempFile("dog-dgf-", ".bin");
        boolean complete = false;
        try {
            var hash = MessageDigest.getInstance("SHA-256");
            try (var raw = Files.newOutputStream(sealed);
                 var output = new DataOutputStream(new java.security.DigestOutputStream(raw, hash));
                 var input = new DataInputStream(Files.newInputStream(source))) {
                output.writeInt(MAGIC);
                output.writeLong(size);
                output.write(prefix);
                for (long offset = 0; offset < size;) {
                    int length = (int)Math.min(BLOCK, size - offset), index = (int)(offset / BLOCK);
                    byte[] plain = new byte[length];
                    input.readFully(plain);
                    output.write(E2ee.cipher(Cipher.ENCRYPT_MODE, key, nonce(prefix, index),
                                             aad(conversation, message, size, index), plain));
                    offset += length;
                }
                if (input.read() != -1) throw new IOException("Cambió el tamaño del archivo durante la lectura");
            }
            if (Files.size(sealed) != expected) throw new IOException("Longitud cifrada inválida");
            complete = true;
            return new Sealed(sealed, E2ee.b64(key), size, HexFormat.of().formatHex(hash.digest()));
        } finally {
            java.util.Arrays.fill(key, (byte)0);
            if (!complete) Files.deleteIfExists(sealed);
        }
    }

    static void decrypt(Path sealed, OutputStream output, String secret, long expectedSize,
                        String conversation, String message) throws Exception {
        if (Files.size(sealed) != limit(expectedSize)) throw new IOException("Longitud cifrada inválida");
        byte[] key = E2ee.raw(secret);
        try (var input = new DataInputStream(Files.newInputStream(sealed))) {
            if (key.length != 32 || input.readInt() != MAGIC || input.readLong() != expectedSize) {
                throw new IOException("Cabecera cifrada inválida");
            }
            byte[] prefix = new byte[8];
            input.readFully(prefix);
            for (long offset = 0; offset < expectedSize;) {
                int count = (int)Math.min(BLOCK, expectedSize - offset), index = (int)(offset / BLOCK);
                byte[] cipher = new byte[count + 16];
                input.readFully(cipher);
                output.write(E2ee.cipher(Cipher.DECRYPT_MODE, key, nonce(prefix, index),
                                          aad(conversation, message, expectedSize, index), cipher));
                offset += count;
            }
            if (input.read() != -1) throw new IOException("Archivo cifrado con datos sobrantes");
        } finally { java.util.Arrays.fill(key, (byte)0); }
    }
}
