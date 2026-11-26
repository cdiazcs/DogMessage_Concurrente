package dogmessenger;

import dogmessenger.chat.Envelope;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HexFormat;
import java.nio.charset.StandardCharsets;

public final class ChatEnvelopeTest {
    public static void main(String[] args) throws Exception {
        var hex = HexFormat.of();
        var rows = Files.readAllLines(Path.of("protocol/fixtures/chat.tsv"));
        for (String row : rows) {
            var fields = row.split("\t");
            byte[] raw = hex.parseHex(fields[2]);
            try {
                var decoded = Envelope.decode(raw, new byte[32]);
                if (!fields[1].equals("1") || !Arrays.equals(raw, Envelope.encode(decoded, new byte[32]))) {
                    throw new AssertionError(fields[0]);
                }
            } catch (IOException error) {
                if (fields[1].equals("1")) throw error;
            }
        }
        var vector = Files.readString(Path.of("protocol/fixtures/chat_pbkdf2.tsv")).strip().split("\t");
        var password = new String(hex.parseHex(vector[1]), StandardCharsets.UTF_8).toCharArray();
        if (!hex.formatHex(Envelope.derive(password, hex.parseHex(vector[0]))).equals(vector[2])) {
            throw new AssertionError("PBKDF2 UTF-8");
        }
        System.out.println("PASS Java CHAT1: " + rows.size() + " fixtures + PBKDF2");
    }
}
