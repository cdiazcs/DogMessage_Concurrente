package dogmessenger;

import dogmessenger.media.BlobServer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;

public final class MediaWireTest {
    public static void main(String[] args) throws Exception {
        var hex = HexFormat.of();
        int count = 0;
        for (String line : Files.readAllLines(Path.of("protocol/fixtures/media.tsv"))) {
            if (line.startsWith("#")) continue;
            String[] fields = line.split("\t", -1);
            byte[] payload = hex.parseHex(fields[4]);
            byte[] body = ByteBuffer.allocate(71 + payload.length)
                .put("DMB1".getBytes(StandardCharsets.US_ASCII))
                .put((byte)(fields[0].equals("status") ? 1 : 2))
                .putLong(0x0102030405060708L).putLong(0x090a0b0c0d0e0f10L)
                .put((byte)'n').put((byte)'2').put(hex.parseHex(fields[2])).putLong(8195)
                .putLong(Long.parseLong(fields[3])).put(payload).array();
            byte[] digest = BlobServer.sign(hex.parseHex(fields[1]), body);
            var wire = ByteBuffer.allocate(4 + body.length + digest.length)
                .putInt(body.length + digest.length).put(body).put(digest);
            if (!MessageDigest.isEqual(wire.array(), hex.parseHex(fields[5]))) throw new AssertionError(fields[0]);
            count++;
        }
        System.out.println("PASS DMB1 JVM: " + count + " fixtures");
    }
}
