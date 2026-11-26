import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import javax.crypto.Cipher;

public final class E2eeVectors {
    public static void main(String[] args) throws Exception {
        var hex = HexFormat.of();
        int count = 0;
        for (String line : Files.readAllLines(Path.of("protocol/fixtures/e2ee.tsv"))) {
            if (line.startsWith("#")) continue;
            String[] row = line.split("\t");
            byte[] key = hex.parseHex(row[1]), param = hex.parseHex(row[2]), data = hex.parseHex(row[3]);
            byte[] result = row[0].equals("gcm") ? E2ee.cipher(Cipher.ENCRYPT_MODE, key, param, "", data)
                         : E2ee.hkdf(key, param, data);
            if (!MessageDigest.isEqual(result, hex.parseHex(row[4]))) throw new AssertionError(row[0]);
            count++;
        }
        System.out.println("PASS Java E2EE: " + count + " vectores");
    }
}
