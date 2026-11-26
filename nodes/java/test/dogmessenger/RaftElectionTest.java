package dogmessenger;

import static dogmessenger.NodeMain.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

public final class RaftElectionTest {
    private static final List<String> IDS = List.of("n1", "n2", "n3");
    private static final LogPosition ORIGIN = new LogPosition(0, 0);
    private static final Pattern EVENT = Pattern.compile(
            "\\[RAFT (\\w+)] role=(\\w+) term=(\\d+) leader=([\\w-]+) event=([\\w-]+)");
    private static final TcpClient CLIENT = new TcpClient();

    private record Event(String node, RaftRole role, long term, String leader, String reason) {}

    @FunctionalInterface
    private interface Attempt<T> { T read() throws Exception; }

    private static final class Cluster implements AutoCloseable {
        private final Path root;
        private final Map<String, Integer> ports = new HashMap<>();
        private final Map<String, Process> processes = new HashMap<>();

        Cluster() throws IOException {
            root = Files.createTempDirectory("java-raft-test-");
            try (var first = new ServerSocket(0); var second = new ServerSocket(0);
                 var third = new ServerSocket(0)) {
                ports.put("n1", first.getLocalPort());
                ports.put("n2", second.getLocalPort());
                ports.put("n3", third.getLocalPort());
            }
        }

        InetSocketAddress endpoint(String id) {
            return new InetSocketAddress("127.0.0.1", ports.get(id));
        }

        private Path log(String id) { return root.resolve(id + ".log"); }

        Map<String, InetSocketAddress> peers(String id) {
            Map<String, InetSocketAddress> peers = new HashMap<>();
            for (String peer : IDS) {
                if (!peer.equals(id)) peers.put(peer, endpoint(peer));
            }
            return peers;
        }

        void seed(String id, List<LogEntry> entries) throws IOException {
            var peers = peers(id);
            try (var node = new RaftNode(id, Set.copyOf(IDS), peers, root.resolve(id))) {
                var vote = new Header(1, MessageCodec.REQUEST_VOTE, UUID.randomUUID(), peers.keySet().iterator().next());
                check(node.requestVote(vote, new RequestVote(5, ORIGIN)).voteGranted(), "Preparar término del log");
            }
            try (var log = new RaftLog(root.resolve(id))) {
                for (LogEntry entry : entries) log.append(entry);
            }
        }

        RaftNode.Snapshot recover(String id) throws IOException {
            try (var node = new RaftNode(id, Set.copyOf(IDS), peers(id), root.resolve(id))) {
                return node.snapshot();
            }
        }

        boolean applied(String id, long index) throws IOException {
            return Files.readAllLines(log(id)).stream().anyMatch(line ->
                    line.contains(" entryIndex=" + index + " ") && line.endsWith("event=applied"));
        }

        List<String> progress(String leader, String peer) throws IOException {
            if (!Files.exists(log(leader))) return List.of();
            return Files.readAllLines(log(leader)).stream()
                    .filter(line -> line.startsWith("[RAFT " + leader + "] peer=" + peer + " ")).toList();
        }

        boolean replicated(String leader, String peer, long index) throws IOException {
            return progress(leader, peer).stream().anyMatch(line -> line.endsWith(
                    " matchIndex=" + index + " event=replicated"));
        }

        void start(String id) throws IOException {
            check(!processes.containsKey(id), "Proceso ya registrado: " + id);
            var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp", System.getProperty("java.class.path"), "dogmessenger.NodeMain", "--id", id,
                    "--host", "127.0.0.1", "--port", ports.get(id).toString(),
                    "--data-dir", root.resolve(id).toString()));
            for (String peer : IDS) {
                if (!peer.equals(id)) command.addAll(List.of("--peer", peer + "=127.0.0.1:" + ports.get(peer)));
            }
            var process = new ProcessBuilder(command).redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.appendTo(log(id).toFile())).start();
            processes.put(id, process);
        }

        List<Event> events(String id) throws IOException {
            if (!Files.exists(log(id))) return List.of();
            var result = new ArrayList<Event>();
            var matcher = EVENT.matcher(Files.readString(log(id)));
            while (matcher.find()) {
                result.add(new Event(matcher.group(1), RaftRole.valueOf(matcher.group(2)),
                        Long.parseLong(matcher.group(3)), matcher.group(4), matcher.group(5)));
            }
            return result;
        }

        Event latest(String id) throws IOException {
            List<Event> events = events(id);
            return events.isEmpty() ? null : events.get(events.size() - 1);
        }

        Event leaderAfter(long term) throws IOException {
            for (String id : IDS) {
                Event event = latest(id);
                Process process = processes.get(id);
                if (process != null && process.isAlive() && event != null
                        && event.role() == RaftRole.LEADER && event.term() > term) return event;
            }
            return null;
        }

        void kill(String id) throws InterruptedException {
            Process process = processes.remove(id);
            process.destroyForcibly();
            check(process.waitFor(5, TimeUnit.SECONDS), "El proceso no terminó: " + id);
        }

        void dump() throws IOException {
            for (String id : IDS) {
                if (Files.exists(log(id))) System.err.print(Files.readString(log(id)));
            }
        }

        void checkOneLeaderPerTerm() throws IOException {
            Map<Long, String> leaders = new HashMap<>();
            for (String id : IDS) {
                for (Event event : events(id)) {
                    if (event.role() == RaftRole.LEADER) {
                        String prior = leaders.putIfAbsent(event.term(), id);
                        check(prior == null || prior.equals(id), "Dos líderes en término " + event.term());
                    }
                }
            }
        }

        @Override public void close() throws IOException {
            for (Process process : processes.values()) process.destroyForcibly();
            try {
                for (Process process : processes.values()) {
                    if (!process.waitFor(5, TimeUnit.SECONDS)) throw new IOException("Proceso de prueba vivo");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(interrupted);
            }
            try (var paths = Files.walk(root)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
            }
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    private static <T> T await(String message, Attempt<T> attempt) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(12);
        do {
            T value = attempt.read();
            if (value != null) return value;
            Thread.sleep(15);
        } while (System.nanoTime() - until < 0);
        throw new AssertionError(message);
    }

    private static Event followerOf(Cluster cluster, String id, Event leader) throws IOException {
        Event event = cluster.latest(id);
        return event != null && event.role() == RaftRole.FOLLOWER && event.term() == leader.term()
                && event.leader().equals(leader.node()) ? event : null;
    }

    private static void electionAndFailover() throws Exception {
        try (var cluster = new Cluster()) {
            try {
                cluster.start("n1");
                Event isolated = await("El aislado no reintentó la elección", () -> {
                    Event event = cluster.latest("n1");
                    return event != null && event.role() == RaftRole.CANDIDATE && event.term() >= 3 ? event : null;
                });
                check(cluster.events("n1").stream().noneMatch(event -> event.role() == RaftRole.LEADER),
                        "Una réplica aislada ganó sin mayoría");
                System.out.println("OK aislado sin mayoría hasta término " + isolated.term());

                cluster.start("n2");
                cluster.start("n3");
                Event first = await("Los tres nodos no convergieron tras la elección inicial", () -> {
                    Event leader = cluster.leaderAfter(0);
                    if (leader == null) return null;
                    for (String id : IDS) {
                        if (!id.equals(leader.node()) && followerOf(cluster, id, leader) == null) return null;
                    }
                    return leader;
                });
                Thread.sleep(1_400); // Supera el máximo electoral de 900 ms.
                for (String id : IDS) {
                    Event now = cluster.latest(id);
                    check(now.term() == first.term(), "Heartbeat dejó cambiar el término: " + now);
                    if (!id.equals(first.node())) check(followerOf(cluster, id, first) != null, "Perdió el líder");
                }
                System.out.println("OK elección y heartbeats: " + first.node() + " término " + first.term());

                cluster.kill(first.node());
                try (var recoveredLog = new RaftLog(cluster.root.resolve(first.node()))) {
                    var entry = recoveredLog.entryAt(recoveredLog.lastLogIndex());
                    check(entry != null && entry.term() == first.term() && entry.type() == RaftLog.EntryType.NO_OP,
                            "NO_OP perdido tras matar al líder");
                }
                Event replacement = await("La mayoría no reeligió después de la caída",
                        () -> cluster.leaderAfter(first.term()));
                check(!replacement.node().equals(first.node()), "El proceso caído sigue como líder");
                cluster.start(first.node());
                await("El antiguo líder no volvió como seguidor", () -> followerOf(cluster, first.node(), replacement));
                Event recovered = cluster.events(first.node()).stream()
                        .filter(event -> event.reason().equals("startup")).reduce((previous, last) -> last).orElseThrow();
                check(recovered.role() == RaftRole.FOLLOWER && recovered.term() == first.term(),
                        "Reinicio no recuperó el término persistido: " + recovered);
                cluster.checkOneLeaderPerTerm();
                System.out.println("OK caída, NO_OP durable, reelección y reinicio: " + replacement.node()
                        + " término " + replacement.term());
            } catch (Exception | AssertionError failure) {
                cluster.dump();
                throw failure;
            }
        }
    }

    private static Payload send(Cluster cluster, String sender, Payload payload) throws IOException {
        return CLIENT.call(cluster.endpoint("n2"), "n2", sender, payload).payload();
    }

    private static void waitStartup(Cluster cluster, long term, int count) throws Exception {
        await("El proceso no recuperó el término " + term, () -> cluster.events("n2").stream()
                .filter(event -> event.reason().equals("startup") && event.term() == term).count() >= count
                ? Boolean.TRUE : null);
    }

    private static void votesAndRestart() throws Exception {
        try (var cluster = new Cluster()) {
            try {
                cluster.start("n2");
                waitStartup(cluster, 0, 1);
                check(send(cluster, "unknown", new RequestVote(200, ORIGIN))
                        .equals(new RpcError(ErrorCode.UNKNOWN_MEMBER)), "Miembro desconocido");
                check(send(cluster, "n1", new RequestVote(100, ORIGIN))
                        .equals(new VoteResponse(100, true)), "Primer voto");
                check(send(cluster, "n3", new RequestVote(100, ORIGIN))
                        .equals(new VoteResponse(100, false)), "Segundo candidato mismo término");
                check(send(cluster, "n1", new RequestVote(99, ORIGIN))
                        .equals(new VoteResponse(100, false)), "Término anterior");

                cluster.kill("n2");
                cluster.start("n2");
                waitStartup(cluster, 100, 1);
                check(send(cluster, "n1", new RequestVote(100, ORIGIN))
                        .equals(new VoteResponse(100, true)), "Voto original tras reinicio");
                check(send(cluster, "n3", new RequestVote(100, ORIGIN))
                        .equals(new VoteResponse(100, false)), "Voto doble tras reinicio");
                check(send(cluster, "n3", new AppendEntries(101, new LogPosition(1, 100), 0, List.of()))
                        .equals(new AppendResponse(101, false, 0)), "Posición previa incompatible");
                check(send(cluster, "n3", new AppendEntries(101, ORIGIN, 0, List.of()))
                        .equals(new AppendResponse(101, true, 0)), "Heartbeat vigente");
                check(send(cluster, "n1", new AppendEntries(100, ORIGIN, 0, List.of()))
                        .equals(new AppendResponse(101, false, 0)), "Heartbeat obsoleto");
                check(send(cluster, "n3", new RequestVote(101, ORIGIN))
                        .equals(new VoteResponse(101, true)), "Término superior limpió el voto previo");
                check(send(cluster, "n3", new AppendEntries(101, ORIGIN, 0, List.of(new NoOpEntry(101))))
                        .equals(new AppendResponse(101, true, 1)), "Replicación del NO_OP vigente");
                System.out.println("OK persistencia de término/voto, rechazo y cambio de término");
            } catch (Exception | AssertionError failure) {
                cluster.dump();
                throw failure;
            }
        }
    }

    private static void higherTermInResponse() throws Exception {
        try (var cluster = new Cluster()) {
            RpcHandler remote = new RpcHandler() {
                @Override public VoteResponse requestVote(Header header, RequestVote vote) {
                    return new VoteResponse(50, false);
                }
            };
            var dispatcher = new RpcDispatcher("n3", Set.copyOf(IDS), remote);
            try (var server = new TcpServer(cluster.endpoint("n3"), dispatcher, 1, 2)) {
                var acceptor = java.util.concurrent.Executors.newSingleThreadExecutor();
                var serving = acceptor.submit(() -> { server.serve(); return null; });
                try {
                    cluster.start("n1");
                    await("El candidato no aceptó el término superior de una respuesta correlacionada", () ->
                            cluster.events("n1").stream()
                                    .filter(event -> event.role() == RaftRole.FOLLOWER && event.term() == 50
                                            && event.reason().equals("higher-term")).findFirst().orElse(null));
                    check(cluster.events("n1").stream().noneMatch(event -> event.role() == RaftRole.LEADER),
                            "Un rechazo se contó como mayoría");
                } finally {
                    acceptor.shutdownNow();
                    serving.get(5, TimeUnit.SECONDS);
                }
            } catch (Exception | AssertionError failure) {
                cluster.dump();
                throw failure;
            }
        }
        System.out.println("OK término superior en respuesta de voto correlacionada");
    }

    private static void delayedHigherTermAfterSameTermStepDown() throws Exception {
        try (var cluster = new Cluster()) {
            var received = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            RpcHandler remote = new RpcHandler() {
                @Override public VoteResponse requestVote(Header header, RequestVote vote) throws IOException {
                    received.countDown();
                    try {
                        if (!release.await(4, TimeUnit.SECONDS)) throw new IOException("Respuesta no liberada");
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IOException(interrupted);
                    }
                    return new VoteResponse(50, false);
                }
            };
            var dispatcher = new RpcDispatcher("n3", Set.copyOf(IDS), remote);
            try (var server = new TcpServer(cluster.endpoint("n3"), dispatcher, 1, 2)) {
                var acceptor = Executors.newSingleThreadExecutor();
                var serving = acceptor.submit(() -> { server.serve(); return null; });
                try {
                    cluster.start("n2");
                    check(received.await(4, TimeUnit.SECONDS), "El voto saliente no llegó");
                    var election = cluster.events("n2").stream().filter(e -> e.reason().equals("election"))
                            .findFirst().orElseThrow();
                    long term = election.term();
                    check(send(cluster, "n1", new AppendEntries(term, ORIGIN, 0, List.of()))
                            .equals(new AppendResponse(term, true, 0)), "El candidato acepta líder del mismo término");
                    check(send(cluster, "n3", new RequestVote(term, ORIGIN)).equals(new VoteResponse(term, false)),
                            "El candidato conserva su voto al pasar a seguidor");
                    release.countDown();
                    await("Respuesta tardía con término superior no persistida", () -> cluster.events("n2").stream()
                            .filter(e -> e.role() == RaftRole.FOLLOWER && e.term() == 50
                                    && e.reason().equals("higher-term")).findFirst().orElse(null));
                    cluster.kill("n2");
                    check(cluster.recover("n2").term() >= 50, "Término de la respuesta tardía recuperado");
                } finally {
                    release.countDown();
                    acceptor.shutdownNow();
                    serving.get(5, TimeUnit.SECONDS);
                }
            } catch (Exception | AssertionError failure) {
                cluster.dump();
                throw failure;
            }
        }
        System.out.println("OK voto conservado y término superior persistido incluso al cambiar de rol");
    }

    private static void replication() throws Exception {
        try (var cluster = new Cluster()) {
            try {
                var prefix = new NoOpEntry(1);
                List<LogEntry> expected = new ArrayList<>();
                expected.add(prefix);
                for (int i = 1; i <= 16; i++) {
                    byte[] bytes = new byte[65_536];
                    bytes[0] = (byte) i;
                    expected.add(new CommandEntry(3, "c1", new UUID(1, i), new ClientCommand(bytes)));
                }
                cluster.seed("n1", expected);
                cluster.seed("n2", List.of(prefix));
                cluster.seed("n3", List.of(prefix, new NoOpEntry(2), new NoOpEntry(2)));
                cluster.start("n1");
                cluster.start("n2");
                Event leader = await("Falta líder con el log más fresco", () -> cluster.leaderAfter(5));
                check(leader.node().equals("n1"), "Se eligió un log atrasado");
                expected.add(new NoOpEntry(leader.term()));
                int initialEnd = expected.size();
                await("El seguidor atrasado no alcanzó el final",
                        () -> cluster.replicated("n1", "n2", initialEnd) ? Boolean.TRUE : null);
                List<String> catchup = cluster.progress("n1", "n2");
                check(catchup.stream().anyMatch(line -> line.endsWith("event=replication-retry")),
                        "Falta retroceso después de rechazar el prefijo");
                check(catchup.stream().filter(line -> line.endsWith("event=replicated")).count() >= 2,
                        "El sufijo de más de 1 MiB necesita varios mensajes DM2");

                cluster.start("n3");
                await("No se reparó el seguidor divergente",
                        () -> cluster.replicated("n1", "n3", initialEnd) ? Boolean.TRUE : null);
                check(cluster.progress("n1", "n3").stream().anyMatch(line -> line.endsWith("event=replication-retry")),
                        "Falta retroceso por términos divergentes");
                cluster.kill("n3");

                var command = new ClientCommand(new byte[] {0, (byte) 0xff, 42});
                var header = new Header(1, MessageCodec.CLIENT_COMMAND, new UUID(2, 1), "c1");
                var request = new Message(header, command);
                expected.add(new CommandEntry(leader.term(), "c1", header.requestId(), command));
                int confirmed = expected.size();
                ClientResult committed = new ClientResult(0, confirmed, null);
                await("No se confirmó el comando con 2 de 3", () ->
                        CLIENT.call(cluster.endpoint("n1"), "n1", request).payload().equals(committed) ? committed : null);
                check(CLIENT.call(cluster.endpoint("n1"), "n1", request).payload().equals(committed),
                        "Reenvío confirmado conserva el índice");
                check(CLIENT.call(cluster.endpoint("n1"), "n1", new Message(header, new ClientCommand(new byte[] {1})))
                        .payload().equals(new ClientResult(3, 0, null)), "ID reutilizado con bytes distintos");
                check(cluster.replicated("n1", "n2", confirmed) && !cluster.replicated("n1", "n3", confirmed),
                        "El compromiso se obtuvo con solo dos nodos");
                check(Files.readString(cluster.log("n1")).matches("(?s).*entryIndex=" + confirmed
                        + " .*effect=true consensusNanos=[0-9]+ event=applied.*"), "Métrica de consenso ausente");

                cluster.kill("n2");
                var pendingHeader = new Header(1, MessageCodec.CLIENT_COMMAND, new UUID(2, 2), "c1");
                var pending = new Message(pendingHeader, new ClientCommand(new byte[] {99}));
                ClientResult retry = new ClientResult(2, 0, null);
                check(CLIENT.call(cluster.endpoint("n1"), "n1", pending).payload().equals(retry),
                        "Un líder aislado no puede confirmar una entrada nueva");
                check(CLIENT.call(cluster.endpoint("n1"), "n1", pending).payload().equals(retry),
                        "El reintento sigue pendiente con un solo nodo");
                cluster.kill("n1");
                var isolated = cluster.recover("n1");
                check(isolated.commitIndex() == confirmed && isolated.lastApplied() == confirmed
                        && isolated.log().size() == confirmed + 1
                        && pendingHeader.requestId().equals(isolated.log().get(confirmed).requestId()),
                        "El reinicio distingue una entrada durable pendiente del prefijo comprometido");

                cluster.start("n2");
                cluster.start("n3");
                Event replacement = await("No hubo reelección tras la caída del líder",
                        () -> cluster.leaderAfter(leader.term()));
                check(replacement.node().equals("n2"), "Una réplica sin el comando confirmado ganó la elección");
                expected.add(new NoOpEntry(replacement.term()));
                int repaired = expected.size();
                await("El NO_OP del nuevo término no comprometió el prefijo",
                        () -> cluster.applied("n2", repaired) ? Boolean.TRUE : null);
                check(CLIENT.call(cluster.endpoint("n2"), "n2", request).payload().equals(committed),
                        "La reelección conserva el índice confirmado");

                cluster.start("n1");
                await("El líder anterior no reemplazó la entrada pendiente",
                        () -> cluster.applied("n1", repaired) ? Boolean.TRUE : null);
                var newCommand = new ClientCommand(new byte[] {100});
                expected.add(new CommandEntry(replacement.term(), "c1", pendingHeader.requestId(), newCommand));
                int finalEnd = expected.size();
                ClientResult finalCommit = new ClientResult(0, finalEnd, null);
                await("Un ID eliminado por reconciliación sigue reservado", () -> CLIENT.call(cluster.endpoint("n2"),
                        "n2", new Message(pendingHeader, newCommand)).payload().equals(finalCommit) ? finalCommit : null);
                for (String id : IDS) {
                    await("El seguidor no aprendió el commit final: " + id,
                            () -> cluster.applied(id, finalEnd) ? Boolean.TRUE : null);
                }
                for (String id : IDS) cluster.kill(id);
                for (String id : IDS) {
                    var recovered = cluster.recover(id);
                    check(recovered.commitIndex() == finalEnd && recovered.lastApplied() == finalEnd
                            && recovered.log().stream().map(RaftLog.LogEntry::entry).toList().equals(expected),
                            "Log o índices divergentes tras reinicio: " + id);
                    check(recovered.appliedCommandIndexes().size() == 18
                            && recovered.appliedCommandIndexes().get(16) == confirmed
                            && recovered.appliedCommandIndexes().get(17) == finalEnd, "Aplicación única y ordenada: " + id);
                }
                System.out.printf("OK 2/3 confirmó índice %d, 1/3 dejó pendiente %d, %s reeligió en término %d.%n",
                        confirmed, confirmed + 1, replacement.node(), replacement.term());
                System.out.println("OK tres procesos: lotes DM2 y reparación; " + finalEnd
                        + " entradas comprometidas iguales y 18 comandos únicos recuperados");
            } catch (Exception | AssertionError failure) {
                cluster.dump();
                throw failure;
            }
        }
    }

    private static void onlyCurrentTermAdvancesCommit() throws Exception {
        try (var cluster = new Cluster()) {
            List<LogEntry> old = java.util.Collections.nCopies(256, new NoOpEntry(3));
            cluster.seed("n1", old);
            var allowCurrentTerm = new AtomicBoolean();
            var blocked = new CountDownLatch(1);
            try (var leader = new RaftNode("n1", Set.copyOf(IDS), cluster.peers("n1"), cluster.root.resolve("n1"));
                 var follower = new RaftNode("n2", Set.copyOf(IDS), cluster.peers("n2"), cluster.root.resolve("n2"))) {
                RpcHandler gate = new RpcHandler() {
                    @Override public VoteResponse requestVote(Header header, RequestVote vote) throws IOException {
                        return follower.requestVote(header, vote);
                    }

                    @Override public AppendResponse appendEntries(Header header, AppendEntries append) throws IOException {
                        boolean containsCurrent = append.entries().stream().anyMatch(entry -> entry.term() == append.term());
                        if (containsCurrent && follower.snapshot().log().size() == old.size() && !allowCurrentTerm.get()) {
                            blocked.countDown();
                            return null;
                        }
                        return follower.appendEntries(header, append);
                    }
                };
                var dispatcher = new RpcDispatcher("n2", Set.copyOf(IDS), gate);
                try (var server = new TcpServer(cluster.endpoint("n2"), dispatcher, 1, 2)) {
                    var acceptor = Executors.newSingleThreadExecutor();
                    var serving = acceptor.submit(() -> { server.serve(); return null; });
                    try {
                        leader.start();
                        check(blocked.await(12, TimeUnit.SECONDS), "No se replicó el lote del término anterior");
                        var before = leader.snapshot();
                        check(before.role() == RaftRole.LEADER && before.log().size() == old.size() + 1
                                && before.commitIndex() == 0 && before.lastApplied() == 0,
                                "Una mayoría con solo entradas antiguas no autoriza el commit");
                        allowCurrentTerm.set(true);
                        await("El NO_OP vigente no comprometió los términos anteriores", () -> {
                            var after = leader.snapshot();
                            return after.commitIndex() == old.size() + 1 && after.lastApplied() == old.size() + 1
                                    ? after : null;
                        });
                    } finally {
                        acceptor.shutdownNow();
                        serving.get(5, TimeUnit.SECONDS);
                    }
                }
            }
        }
        System.out.println("OK mayoría con 256 entradas antiguas no avanza el commit hasta replicar el NO_OP vigente");
    }

    public static void main(String[] args) throws Exception {
        var local = new LogPosition(7, 3);
        check(!RaftNode.upToDate(new LogPosition(100, 2), local), "El término prevalece sobre el índice");
        check(!RaftNode.upToDate(new LogPosition(6, 3), local), "Índice atrasado para igual término");
        check(RaftNode.upToDate(local, local), "Log igual");
        check(RaftNode.upToDate(new LogPosition(1, 4), local), "Término más reciente");
        System.out.println("OK frescura de log por término e índice");
        votesAndRestart();
        higherTermInResponse();
        delayedHigherTermAfterSameTermStepDown();
        electionAndFailover();
        replication();
        onlyCurrentTermAdvancesCommit();
    }
}
