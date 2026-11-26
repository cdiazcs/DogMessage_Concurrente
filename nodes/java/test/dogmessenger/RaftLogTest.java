package dogmessenger;

import static dogmessenger.NodeMain.*;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class RaftLogTest {
    private static final LogPosition ORIGIN = new LogPosition(0, 0);
    private static final UUID REQUEST = new UUID(1, 2);

    @FunctionalInterface
    private interface IoAction { void run() throws IOException; }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    private static void fails(IoAction action, Class<? extends Exception> expected) throws IOException {
        try {
            action.run();
        } catch (IOException | RuntimeException error) {
            if (expected.isInstance(error)) return;
            throw error;
        }
        throw new AssertionError("Se esperaba " + expected.getSimpleName());
    }

    private static CommandEntry command(long term, byte[] payload) {
        return new CommandEntry(term, "c1", REQUEST, new ClientCommand(payload));
    }

    private static void roundTrip(Path directory) throws Exception {
        byte[] payload = new byte[65_536];
        payload[0] = (byte) 0xff;
        payload[payload.length - 1] = (byte) 0x80;
        RaftLog.LogEntry noOp;
        RaftLog.LogEntry command;
        try (var log = new RaftLog(directory)) {
            check(log.lastPosition().equals(ORIGIN), "Origen sin entradas");
            check(log.entryAt(-1) == null && log.entryAt(0) == null && log.entryAt(1) == null, "Índice ausente");
            noOp = log.append(new NoOpEntry(2));
            command = log.append(command(3, payload));
            check(noOp.index() == 1 && noOp.term() == 2 && noOp.type() == RaftLog.EntryType.NO_OP
                    && noOp.requestId() == null && noOp.clientId() == null && noOp.payload().length == 0, "NO_OP");
            check(command.index() == 2 && command.term() == 3 && command.type() == RaftLog.EntryType.COMMAND
                    && REQUEST.equals(command.requestId()) && "c1".equals(command.clientId())
                    && Arrays.equals(payload, command.payload()), "COMMAND");
            payload[0] = 0;
            byte[] copy = command.payload();
            copy[copy.length - 1] = 0;
            check(command.payload()[0] == (byte) 0xff && command.payload()[copy.length - 1] == (byte) 0x80,
                    "Payload inmutable");
            fails(() -> log.append(new NoOpEntry(2)), IllegalArgumentException.class);
            fails(() -> {
                try (var competing = new RaftLog(directory)) {
                    throw new AssertionError("Segundo escritor: " + competing.lastPosition());
                }
            }, IOException.class);
            Process competing = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp", System.getProperty("java.class.path"), RaftLogTest.class.getName(), directory.toString())
                    .inheritIO().start();
            try {
                check(competing.waitFor(5, TimeUnit.SECONDS) && competing.exitValue() == 0,
                        "El bloqueo debe sobrevivir al intento concurrente en la JVM original");
            } finally {
                if (competing.isAlive()) competing.destroyForcibly();
            }
            check(log.lastLogIndex() == 2 && log.lastLogTerm() == 3, "Posición durable");
        }
        try (var log = new RaftLog(directory)) {
            check(noOp.equals(log.entryAt(1)) && command.equals(log.entryAt(2)), "Entradas tras reapertura");
            check(log.lastPosition().equals(new LogPosition(2, 3)), "Posición tras reapertura");
            check(log.append(new NoOpEntry(3)).index() == 3, "Continuación tras reinicio");
        }
        System.out.println("OK NO_OP y COMMAND, payload inmutable y recuperación con un solo escritor");
    }

    private static void truncatedTail(Path directory) throws IOException {
        Path file = directory.resolve("raft-log.bin");
        int durableSize;
        try (var log = new RaftLog(directory)) {
            log.append(new NoOpEntry(1));
            durableSize = Math.toIntExact(Files.size(file));
            log.append(command(2, new byte[] {0, (byte) 0xff, 17}));
        }
        byte[] complete = Files.readAllBytes(file);
        for (int cut = durableSize + 1; cut < complete.length; cut++) {
            Files.write(file, Arrays.copyOf(complete, cut));
            try (var log = new RaftLog(directory)) {
                check(log.lastPosition().equals(new LogPosition(1, 1)), "Cola parcial recuperada en corte " + cut);
                check(Files.size(file) == durableSize, "El truncamiento debe persistir");
                check(log.append(new NoOpEntry(4)).index() == 2, "Continuación luego de truncar");
            }
            try (var log = new RaftLog(directory)) {
                check(log.lastPosition().equals(new LogPosition(2, 4)), "Continuación durable tras reparar");
            }
        }
        Files.write(file, Arrays.copyOf(complete, 3));
        try (var log = new RaftLog(directory)) {
            check(log.lastPosition().equals(ORIGIN) && Files.size(file) == 0, "Primera escritura incompleta");
        }
        System.out.println("OK " + (complete.length - durableSize - 1)
                + " cortes dentro de un registro y primera escritura incompleta");
    }

    private static void corruptRecord(Path directory) throws IOException {
        Path file = directory.resolve("raft-log.bin");
        int lastStart;
        try (var log = new RaftLog(directory)) {
            log.append(new NoOpEntry(1));
            lastStart = Math.toIntExact(Files.size(file));
            log.append(new NoOpEntry(2));
        }
        byte[] original = Files.readAllBytes(file);
        for (int corruptedOffset : new int[] {4, lastStart + 4, lastStart + 12, original.length - 1}) {
            byte[] broken = original.clone();
            broken[corruptedOffset] ^= 1;
            Files.write(file, broken);
            fails(() -> {
                try (var log = new RaftLog(directory)) {
                    throw new AssertionError("Se abrió un registro corrupto: " + log.lastPosition());
                }
            }, IOException.class);
            check(Arrays.equals(broken, Files.readAllBytes(file)), "La corrupción debe conservarse");
        }
        Files.write(file, original);
        try (var log = new RaftLog(directory)) {
            check(log.lastPosition().equals(new LogPosition(2, 2)), "El fallo debe liberar el archivo");
        }
        System.out.println("OK corrupción de cabecera y CRC detectada sin borrar registros completos");
    }

    private static void concurrentAppends(Path directory) throws Exception {
        var writers = Executors.newFixedThreadPool(8);
        try (var log = new RaftLog(directory)) {
            List<Future<RaftLog.LogEntry>> results = new ArrayList<>();
            for (int i = 0; i < 64; i++) {
                int number = i;
                results.add(writers.submit(() -> {
                    var appended = log.append(command(8, ByteBuffer.allocate(4).putInt(number).array()));
                    LogPosition observed = log.lastPosition();
                    check(observed.index() >= appended.index() && observed.term() == appended.term()
                            && appended.equals(log.entryAt(appended.index())), "Vista concurrente consistente");
                    return appended;
                }));
            }
            Set<Long> indexes = new java.util.HashSet<>();
            for (int i = 0; i < results.size(); i++) {
                var entry = results.get(i).get(10, TimeUnit.SECONDS);
                check(indexes.add(entry.index()) && ByteBuffer.wrap(entry.payload()).getInt() == i,
                        "Índice único y payload correspondiente");
            }
            check(log.lastPosition().equals(new LogPosition(64, 8)), "Contigüidad en concurrencia");
        } finally {
            writers.shutdownNow();
        }
        try (var log = new RaftLog(directory)) {
            check(log.lastPosition().equals(new LogPosition(64, 8)), "Todas las escrituras sobreviven");
        }
        System.out.println("OK 64 escrituras concurrentes y reapertura");
    }

    private static void raftIntegration(Path directory) throws IOException {
        Set<String> members = Set.of("n1", "n2");
        var peers = Map.of("n2", new InetSocketAddress("127.0.0.1", 12345));
        var vote = new Header(1, MessageCodec.REQUEST_VOTE, REQUEST, "n2");
        var append = new Header(1, MessageCodec.APPEND_ENTRIES, REQUEST, "n2");
        try (var node = new RaftNode("n1", members, peers, directory)) {
            check(node.requestVote(vote, new RequestVote(5, ORIGIN)).voteGranted(), "Directorio fase III");
        }
        try (var log = new RaftLog(directory)) {
            log.append(new NoOpEntry(4));
            log.append(command(5, new byte[] {1}));
        }
        try (var node = new RaftNode("n1", members, peers, directory)) {
            check(node.requestVote(vote, new RequestVote(6, new LogPosition(9, 4)))
                    .equals(new VoteResponse(6, false)), "Rechazar término de log atrasado");
            check(node.requestVote(vote, new RequestVote(6, new LogPosition(1, 5)))
                    .equals(new VoteResponse(6, false)), "Rechazar índice atrasado");
            check(node.requestVote(vote, new RequestVote(6, new LogPosition(2, 5)))
                    .equals(new VoteResponse(6, true)), "Conceder voto al log igual");
            check(node.appendEntries(append, new AppendEntries(6, new LogPosition(1, 4), 0, List.of()))
                    .equals(new AppendResponse(6, true, 1)), "Heartbeat usa un prefijo recuperado");
            check(node.appendEntries(append, new AppendEntries(6, new LogPosition(1, 5), 0, List.of()))
                    .equals(new AppendResponse(6, false, 0)), "Heartbeat rechaza término incompatible");
            check(node.appendEntries(append, new AppendEntries(6, new LogPosition(2, 5), 0, List.of(new NoOpEntry(6))))
                    .equals(new AppendResponse(6, true, 3)), "El seguidor acepta el lote sobre el prefijo validado");
        }
        try (var log = new RaftLog(directory)) {
            check(log.lastPosition().equals(new LogPosition(3, 6)), "La entrada replicada se recupera");
            log.append(new NoOpEntry(7));
        }
        fails(() -> {
            try (var node = new RaftNode("n1", members, peers, directory)) {
                throw new AssertionError("Log con término futuro: " + node.requestVote(vote, new RequestVote(7, ORIGIN)));
            }
        }, IOException.class);
        System.out.println("OK votos, heartbeats y escritura de réplica con log recuperado");
    }

    private static void mergeAndBatch(Path directory) throws IOException {
        var oldCommand = command(2, new byte[] {1});
        var newCommand = command(3, new byte[] {2});
        var prefix = new LogPosition(1, 1);
        try (var log = new RaftLog(directory)) {
            log.append(new NoOpEntry(1));
            log.append(oldCommand);
            log.append(new NoOpEntry(2));
            check(!log.merge(new AppendEntries(3, new LogPosition(1, 2), 0, List.of(newCommand)), 0),
                    "Rechazo cuando el término previo difiere");
            check(log.merge(new AppendEntries(3, prefix, 0, List.of(oldCommand)), 0)
                    && log.lastLogIndex() == 3, "Reenvío coincidente conserva el sufijo");
            check(log.merge(new AppendEntries(3, prefix, 0, List.of(newCommand)), 0)
                    && log.lastLogIndex() == 2, "El conflicto recorta todo el sufijo");
            check(log.merge(new AppendEntries(3, prefix, 0, List.of(newCommand)), 0)
                    && log.lastLogIndex() == 2, "Reenvío después de reparar");
        }
        try (var log = new RaftLog(directory)) {
            check(log.lastPosition().equals(new LogPosition(2, 3)) && log.entryAt(2).entry().equals(newCommand),
                    "Sufijo corregido sobrevive al reinicio");
            for (int i = 0; i < 256; i++) log.append(new NoOpEntry(3));
            AppendEntries first = log.batch(3, 1, "n1", 0);
            AppendEntries second = log.batch(3, first.lastIndex() + 1, "n1", 0);
            check(first.entries().size() == 256 && second.entries().size() == 2
                    && second.prevLog().equals(new LogPosition(256, 3)), "Límite por cantidad");
            check(log.batch(3, 259, "n1", 0).entries().isEmpty(), "Heartbeat después de ponerse al día");
        }
        System.out.println("OK reparación durable, reenvío idempotente y límite de 256 entradas");
    }

    private static void committedRecovery(Path directory) throws IOException {
        Files.createDirectories(directory);
        try (var legacy = new DataOutputStream(Files.newOutputStream(directory.resolve("term-state.bin")))) {
            legacy.writeInt(0x44525331);
            legacy.writeUTF("n1");
            legacy.writeUTF("n1,n2");
            legacy.writeLong(5);
            legacy.writeUTF("n2");
        }
        Set<String> members = Set.of("n1", "n2");
        var peers = Map.of("n2", new InetSocketAddress("127.0.0.1", 12345));
        var append = new Header(1, MessageCodec.APPEND_ENTRIES, REQUEST, "n2");
        var first = command(6, new byte[] {1});
        var second = new CommandEntry(6, "c1", new UUID(1, 3), new ClientCommand(new byte[] {2}));
        try (var node = new RaftNode("n1", members, peers, directory)) {
            check(node.snapshot().term() == 5 && node.snapshot().commitIndex() == 0, "Migración DRS1");
            check(node.appendEntries(append, new AppendEntries(6, ORIGIN, 99, List.of(first, first, second)))
                    .equals(new AppendResponse(6, true, 3)), "Commit limitado al final recibido");
            var snapshot = node.snapshot();
            check(snapshot.commitIndex() == 3 && snapshot.lastApplied() == 3
                    && snapshot.appliedCommandIndexes().equals(List.of(1L, 3L)), "Efecto único y ordenado");
            node.appendEntries(append, new AppendEntries(6, new LogPosition(3, 6), 2, List.of(new NoOpEntry(6))));
            node.appendEntries(append, new AppendEntries(6, new LogPosition(3, 6), 99, List.of()));
            check(node.snapshot().commitIndex() == 3 && node.snapshot().log().size() == 4,
                    "Un heartbeat no compromete un sufijo que no valida, y un aviso viejo no baja el índice");
            fails(() -> node.appendEntries(append, new AppendEntries(7, ORIGIN, 0, List.of(new NoOpEntry(7)))),
                    IOException.class);
            check(node.snapshot().log().equals(List.of(new RaftLog.LogEntry(1, first), new RaftLog.LogEntry(2, first),
                    new RaftLog.LogEntry(3, second), new RaftLog.LogEntry(4, new NoOpEntry(6)))),
                    "Un conflicto jamás recorta una entrada comprometida");
        }
        try (var node = new RaftNode("n1", members, peers, directory)) {
            var snapshot = node.snapshot();
            check(snapshot.term() == 7 && snapshot.commitIndex() == 3 && snapshot.lastApplied() == 3
                    && snapshot.appliedCommandIndexes().equals(List.of(1L, 3L)) && snapshot.log().size() == 4,
                    "DRS2 recupera el índice y reconstruye efectos sin aplicar lo pendiente");
        }
        System.out.println("OK DRS1→DRS2, deduplicación al aplicar y protección del prefijo comprometido");
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 1) {
            try (var log = new RaftLog(Path.of(args[0]))) {
                throw new AssertionError("Un segundo proceso abrió el log: " + log.lastPosition());
            } catch (IOException locked) {
                check(locked.getMessage().startsWith("Log Raft en uso"), "Error de apertura inesperado: " + locked);
            }
            return;
        }
        Path root = Files.createTempDirectory("raft-log-test-");
        try {
            roundTrip(root.resolve("round-trip"));
            truncatedTail(root.resolve("truncated"));
            corruptRecord(root.resolve("corruption"));
            concurrentAppends(root.resolve("concurrent"));
            raftIntegration(root.resolve("raft"));
            mergeAndBatch(root.resolve("merge"));
            committedRecovery(root.resolve("commit"));
        } finally {
            try (var paths = Files.walk(root)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
            }
        }
    }
}
