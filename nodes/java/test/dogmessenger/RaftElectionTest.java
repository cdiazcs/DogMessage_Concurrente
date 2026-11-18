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
import java.util.concurrent.TimeUnit;
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
                System.out.println("OK caída, reelección y reinicio: " + replacement.node()
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
                        .equals(new AppendResponse(101, false, 0)), "Replicación fuera de esta fase");
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

    public static void main(String[] args) throws Exception {
        var local = new LogPosition(7, 3);
        check(!RaftNode.upToDate(new LogPosition(100, 2), local), "El término prevalece sobre el índice");
        check(!RaftNode.upToDate(new LogPosition(6, 3), local), "Índice atrasado para igual término");
        check(RaftNode.upToDate(local, local), "Log igual");
        check(RaftNode.upToDate(new LogPosition(1, 4), local), "Término más reciente");
        System.out.println("OK frescura de log por término e índice");
        votesAndRestart();
        higherTermInResponse();
        electionAndFailover();
    }
}
