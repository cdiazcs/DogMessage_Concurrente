package dogmessenger;

import dogmessenger.twopc.Engine;
import dogmessenger.twopc.Wire;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.UUID;

public final class TwoPcEngineTest {
    private static int call(Engine engine, int kind, Wire.Tx tx) throws IOException {
        var head = new Wire.Header(2, kind, UUID.randomUUID(), "n1");
        return Wire.correlate(engine.dispatch(Wire.encode(head, out -> Wire.transaction(out, tx))),
                head, "n2", Wire.reference(tx)).status();
    }

    private static void check(boolean condition) {
        if (!condition) throw new AssertionError("Estado 2PC JVM incorrecto");
    }

    public static void main(String[] args) throws Exception {
        Path root = Files.createTempDirectory("2pc java ");
        var peers = Map.of("n1", new InetSocketAddress("127.0.0.1", 9101));
        var tx = new Wire.Tx(1, UUID.randomUUID(), "n1", "client", UUID.randomUUID(), new byte[]{0, -1});
        try {
            try (var engine = new Engine("n2", "n1", peers, root, false)) {
                check(call(engine, Wire.COMMIT, tx) == 0);
                check(call(engine, Wire.PREPARE, tx) == 1 && call(engine, Wire.PREPARE, tx) == 1);
                try {
                    new Engine("n2", "n1", peers, root, true).close();
                    throw new AssertionError("Bloqueo ausente");
                } catch (java.nio.channels.OverlappingFileLockException expected) {
                    check(true);
                }
            }
            try (var engine = new Engine("n2", "n1", peers, root, false)) {
                check(call(engine, Wire.COMMIT, tx) == 1 && call(engine, Wire.COMMIT, tx) == 1);
                check(call(engine, Wire.ABORT, tx) == 0);
                var aborted = new Wire.Tx(2, UUID.randomUUID(), "n1", "client", UUID.randomUUID(), new byte[]{1});
                check(call(engine, Wire.ABORT, aborted) == 1 && call(engine, Wire.ABORT, aborted) == 1);
            }
            try (var engine = new Engine("n2", "n1", peers, root, false)) {
                check(call(engine, Wire.COMMIT, tx) == 1);
                Files.delete(root.resolve("two-pc-jvm.bin"));
                Files.createDirectory(root.resolve("two-pc-jvm.bin"));
                var next = new Wire.Tx(3, UUID.randomUUID(), "n1", "client", UUID.randomUUID(), new byte[]{1});
                try {
                    call(engine, Wire.PREPARE, next);
                    throw new AssertionError("YES sin persistencia");
                } catch (IOException expected) {
                    check(engine.dispatch(Wire.encode(new Wire.Header(2, 2, UUID.randomUUID(), "n1"),
                            out -> Wire.transaction(out, next))) == null);
                }
            }
        } finally {
            try (var paths = Files.walk(root)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
            }
        }
        System.out.println("PASS Java 2PC recuperación, exclusión y persistencia");
    }
}
