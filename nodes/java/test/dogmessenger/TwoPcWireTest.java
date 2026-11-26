package dogmessenger;

import dogmessenger.twopc.Wire;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HexFormat;

public final class TwoPcWireTest {
    public static void main(String[] args) throws Exception {
        int count = 0;
        for (String row : Files.readAllLines(Path.of("protocol/fixtures/two_pc.tsv"))) {
            String[] fields = row.split("\t");
            byte[] frame = HexFormat.of().parseHex(fields[2]);
            byte[] body = Arrays.copyOfRange(frame, 4, frame.length);
            if (ByteBuffer.wrap(frame).getInt() != body.length) throw new AssertionError(fields[0]);
            try {
                byte[] encoded = Wire.roundTrip(body);
                if (!fields[1].equals("1") || !Arrays.equals(encoded, body)) throw new AssertionError(fields[0]);
            } catch (IOException invalid) {
                if (fields[1].equals("1")) throw invalid;
            }
            count++;
        }
        System.out.println("PASS Java fixtures 2PC: " + count);
    }
}
