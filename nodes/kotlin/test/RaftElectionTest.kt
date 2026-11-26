import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread

object RaftElectionTest {
    private val local = InetAddress.getByName("127.0.0.1")

    private class Peer(val id: String, handler: (Socket, Dm2Protocol.Header, Dm2Protocol.Request) -> Unit) : AutoCloseable {
        private val server = ServerSocket(0, 10, local)
        val address = InetSocketAddress(local, server.localPort)
        private val active = AtomicReference<Socket>()
        private val failure = AtomicReference<Throwable>()
        private val worker = thread(isDaemon = true) {
            try {
                while (!server.isClosed) {
                    server.accept().use { socket ->
                        active.set(socket)
                        val body = Dm2Protocol.readFrame(socket, 1000)
                        val header = Dm2Protocol.leerHeader(body)
                        handler(socket, header, Dm2Protocol.readRequest(body, header))
                        active.set(null)
                    }
                }
            } catch (error: Throwable) {
                if (!server.isClosed) failure.set(error)
            }
        }

        override fun close() {
            server.close()
            active.get()?.close()
            worker.join(3000)
            check(!worker.isAlive) { "Peer $id sin cerrar" }
            failure.get()?.let { throw AssertionError("Peer $id", it) }
        }
    }

    private fun reply(socket: Socket, head: Dm2Protocol.Header, request: Dm2Protocol.Request, id: String,
                      term: Long, flag: Int, token: ByteArray = head.requestId, copies: Int = 1) {
        val frames = ByteArrayOutputStream()
        repeat(copies) {
            Dm2Protocol.writeFrame(DataOutputStream(frames), id, token, request.type or 0x80) {
                it.writeLong(term)
                it.writeByte(flag)
                if (request is Dm2Protocol.Append) it.writeLong(if (flag == 1) request.lastIndex else 0)
            }
        }
        frames.writeTo(socket.getOutputStream())
    }

    private fun eventually(check: () -> Boolean) {
        val limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(3)
        while (!check()) {
            kotlin.check(System.nanoTime() < limit) { "Venció la espera de la prueba" }
            Thread.sleep(5)
        }
    }

    private fun scenario(label: String, body: (Path) -> Unit) {
        val path = Files.createTempDirectory("kotlin election ")
        try {
            body(path)
            println("PASS $label")
        } finally {
            Files.walk(path).use { files ->
                files.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) }
            }
        }
    }

    private class TestTime {
        private val time = AtomicLong()
        fun nanos(): Long = TimeUnit.MILLISECONDS.toNanos(time.get())
        fun set(milliseconds: Long) { time.set(milliseconds) }
    }

    private fun electionAndRetry(dir: Path) {
        val time = TestTime()
        val votes = CountDownLatch(2)
        val heartbeat = CountDownLatch(1)
        Peer("c1") { socket, header, request ->
            when (request) {
                is Dm2Protocol.Vote -> {
                    if (request.term == 1L) {
                        reply(socket, header, request, "c1", 99, 1, Dm2Protocol.generarRequestId())
                        votes.countDown()
                    } else reply(socket, header, request, "c1", request.term, 1)
                }
                is Dm2Protocol.Append -> {
                    check(request.term == 2L && request.index == 0L && request.commit == 0L)
                    reply(socket, header, request, "c1", request.term, 1)
                    heartbeat.countDown()
                }
                else -> error("Comando inesperado")
            }
        }.use { first ->
            Peer("c2") { socket, header, request ->
                when (request) {
                    is Dm2Protocol.Vote -> {
                        if (request.term == 1L) {
                            reply(socket, header, request, "c2", 98, 2)
                            votes.countDown()
                        } else reply(socket, header, request, "c2", request.term, 0)
                    }
                    is Dm2Protocol.Append -> reply(socket, header, request, "c2", request.term, 1)
                    else -> error("Comando inesperado")
                }
            }.use { second ->
                RaftStorage(dir, "n3").use { storage ->
                    val peers = mapOf(first.id to first.address, second.id to second.address)
                    PersistentRaftNode("n3", peers, storage, time::nanos) { 500 }.use { node ->
                        time.set(499)
                        node.tick()
                        check(node.inspect().startsWith("role=FOLLOWER term=0"))
                        time.set(500)
                        node.tick()
                        check(votes.await(2, TimeUnit.SECONDS))
                        Thread.sleep(50)
                        check(node.inspect().startsWith("role=CANDIDATE term=1 votedFor=n3"))
                        check(storage.snapshot().votedFor == "n3")
                        time.set(1000)
                        node.tick()
                        eventually { node.inspect().startsWith("role=LEADER term=2") }
                        time.set(1125)
                        node.tick()
                        check(heartbeat.await(2, TimeUnit.SECONDS))
                        val state = storage.snapshot()
                        check(state.commitIndex == 0L && state.log.single().isNoOp && state.log.single().term == 2L)
                        println("Elección con 2 de 3 votos: ${node.inspect().lineSequence().first()}")
                    }
                    PersistentRaftNode("n3", peers, storage).use {
                        check(it.inspect().startsWith("role=FOLLOWER term=2 votedFor=n3"))
                    }
                }
            }
        }
    }

    private fun duplicateAndUnknownDoNotWin(dir: Path) {
        val time = TestTime()
        val processed = CountDownLatch(4)
        fun peer(id: String, sender: String, flag: Int, duplicate: Boolean = false): Peer =
            Peer(id) { socket, header, request ->
                when (request) {
                    is Dm2Protocol.Vote -> {
                        val replyId = if (request.term == 1L) sender else id
                        reply(socket, header, request, replyId, request.term, flag, copies = if (duplicate) 2 else 1)
                        processed.countDown()
                    }
                    is Dm2Protocol.Append -> reply(socket, header, request, id, request.term, 1)
                    else -> error("Comando inesperado")
                }
            }
        peer("c1", "c1", 1, true).use { a ->
            peer("c2", "intruso", 1).use { b ->
                peer("c3", "c3", 0).use { c ->
                    peer("c4", "c4", 0).use { d ->
                        val peers = listOf(a, b, c, d).associate { it.id to it.address }
                        RaftStorage(dir, "n3").use { storage ->
                            PersistentRaftNode("n3", peers, storage, time::nanos) { 500 }.use { node ->
                                time.set(500)
                                node.tick()
                                check(processed.await(2, TimeUnit.SECONDS))
                                Thread.sleep(50)
                                check(node.inspect().startsWith("role=CANDIDATE term=1"))
                                check(storage.snapshot().log.isEmpty())
                                time.set(1000)
                                node.tick()
                                eventually { node.inspect().startsWith("role=LEADER term=2") }
                            }
                        }
                    }
                }
            }
        }
    }

    private fun delayedReply(dir: Path, remoteTerm: Long) {
        val time = TestTime()
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        Peer("c1") { socket, header, request ->
            if ((request as Dm2Protocol.Vote).term == 1L) {
                started.countDown()
                check(release.await(2, TimeUnit.SECONDS))
                reply(socket, header, request, "c1", remoteTerm, 1)
            }
        }.use { peer ->
            RaftStorage(dir, "n3").use { storage ->
                PersistentRaftNode("n3", mapOf(peer.id to peer.address), storage, time::nanos) { 500 }.use { node ->
                    time.set(500)
                    node.tick()
                    try {
                        check(started.await(2, TimeUnit.SECONDS))
                        val term = if (remoteTerm == 1L) 2L else 1L
                        check(node.appendEntries(peer.id, Dm2Protocol.Append(term, 0, 0, 0)).success)
                        if (term == 1L) check(storage.snapshot().votedFor == "n3")
                    } finally {
                        release.countDown()
                    }
                    if (remoteTerm > 1L) eventually { storage.snapshot().currentTerm == remoteTerm }
                    else Thread.sleep(80)
                    check(node.inspect().startsWith("role=FOLLOWER term=${maxOf(2, remoteTerm)}"))
                    check(storage.snapshot().votedFor == null)
                }
            }
        }
    }

    private fun heartbeatAndCommit(dir: Path) {
        val time = TestTime()
        val dead = InetSocketAddress(local, 1)
        RaftStorage(dir, "n3").use { storage ->
            storage.save(RaftState(3, null, 0, listOf(EntradaLog(1, 2), EntradaLog(2, 3))))
            PersistentRaftNode("n3", mapOf("c1" to dead), storage, time::nanos) { 500 }.use { node ->
                check(!node.requestVote("c1", 4, 20, 2).granted)
                check(!node.requestVote("c1", 4, 1, 3).granted)
                time.set(450)
                check(node.appendEntries("c1", Dm2Protocol.Append(4, 1, 2, 50)).matchIndex == 1L)
                check(storage.snapshot().commitIndex == 1L)
                check(!node.appendEntries("c1", Dm2Protocol.Append(4, 2, 2, 50)).success)
                check(!node.appendEntries("c1", Dm2Protocol.Append(4, 3, 3, 50)).success)
                check(!node.appendEntries("c1", Dm2Protocol.Append(4, 2, 3, 50, listOf(EntradaLog(3, 4)))).success)
                check(storage.snapshot().log.size == 2 && storage.snapshot().commitIndex == 1L)
                time.set(949)
                check(!node.appendEntries("c1", Dm2Protocol.Append(3, 2, 3, 2)).success)
                node.tick()
                check(node.inspect().startsWith("role=FOLLOWER term=4"))
                time.set(950)
                node.tick()
                check(node.inspect().startsWith("role=CANDIDATE term=5 votedFor=n3"))
                println("Tras perder heartbeats: ${node.inspect().lineSequence().first()}")
                check(node.appendEntries("c1", Dm2Protocol.Append(5, 2, 3, 2)).success)
                check(!node.requestVote("c1", 5, 2, 3).granted)
                check(storage.snapshot().votedFor == "n3")
                check(node.inspect().startsWith("role=FOLLOWER term=5"))
            }
        }
    }

    private fun commandRejection(dir: Path) {
        val time = TestTime()
        RaftStorage(dir, "n3").use { storage ->
            PersistentRaftNode("n3", emptyMap(), storage, time::nanos) { 500 }.use { node ->
                time.set(500)
                node.tick()
                check(node.inspect().startsWith("role=LEADER term=1"))
                val request = ByteArrayOutputStream()
                val token = Dm2Protocol.generarRequestId()
                Dm2Protocol.writeFrame(DataOutputStream(request), "client", token, Dm2Protocol.TYPE_CLIENT_COMMAND) {
                    it.writeInt(1)
                    it.writeByte(42)
                }
                val reply = ByteArrayOutputStream()
                node.handleDm2(request.toByteArray().copyOfRange(4, request.size()), DataOutputStream(reply))
                val input = DataInputStream(reply.toByteArray().inputStream())
                val body = input.readNBytes(input.readInt())
                val header = Dm2Protocol.leerHeader(body)
                check(header.type == Dm2Protocol.TYPE_CLIENT_RESULT && header.requestId.contentEquals(token))
                val payload = DataInputStream(body.inputStream())
                payload.skipNBytes(25)
                check(payload.readUnsignedByte() == 2 && payload.readLong() == 0L && payload.readUnsignedByte() == 0)
                check(storage.snapshot().commitIndex == 0L && storage.snapshot().log.single().isNoOp)
            }
        }
    }

    private fun boundAndClose(dir: Path) {
        val time = TestTime()
        val connected = CountDownLatch(1)
        val disconnected = CountDownLatch(1)
        val requests = AtomicInteger()
        Peer("c1") { socket, _, _ ->
            requests.incrementAndGet()
            connected.countDown()
            socket.soTimeout = 2000
            if (socket.getInputStream().read() == -1) disconnected.countDown()
        }.use { peer ->
            RaftStorage(dir, "n3").use { storage ->
                PersistentRaftNode("n3", mapOf(peer.id to peer.address), storage, time::nanos) { 500 }.use { node ->
                    time.set(500)
                    node.tick()
                    check(connected.await(2, TimeUnit.SECONDS))
                    repeat(10) {
                        time.set((it + 2L) * 500)
                        node.tick()
                    }
                    check(requests.get() == 1)
                    node.close()
                    check(disconnected.await(2, TimeUnit.SECONDS))
                }
            }
        }
    }

    private fun exhaustedTerm(dir: Path) {
        val time = TestTime()
        RaftStorage(dir, "n3").use { storage ->
            storage.save(RaftState(Long.MAX_VALUE, "n3"))
            PersistentRaftNode("n3", emptyMap(), storage, time::nanos) { 500 }.use { node ->
                time.set(500)
                node.tick()
                check(storage.snapshot().currentTerm == Long.MAX_VALUE)
                check(node.inspect().startsWith("role=FOLLOWER term=${Long.MAX_VALUE}"))
            }
        }
    }

    private fun processFailover(dir: Path) {
        val ids = listOf("e1", "e2", "e3")
        val listeners = ids.map { ServerSocket(0, 1, local) }
        val ports = ids.zip(listeners.map { it.localPort }).toMap()
        listeners.forEach { it.close() }
        val java = Path.of(System.getProperty("java.home"), "bin", "java").toString()
        val classpath = System.getProperty("java.class.path")
        val processes = mutableMapOf<String, Process>()
        val leadership = Regex("""role=LEADER term=(\d+) leader=\w+ event=majority commitIndex=0""")
        fun leaders(exclude: String? = null): List<Pair<String, Long>> = processes.keys.filter { it != exclude }
            .mapNotNull { id ->
                val log = Files.readString(dir.resolve("$id.log"))
                leadership.findAll(log).lastOrNull()?.let { id to it.groupValues[1].toLong() }
            }
        try {
            for (id in ids) {
                val args = mutableListOf(java, "-cp", classpath, "NodeMainKt", "--id", id,
                    "--host", "127.0.0.1", "--port", ports.getValue(id).toString(),
                    "--data-dir", dir.resolve(id).toString(), "--experimental-raft", "--members", ids.joinToString(","))
                for (peer in ids.filter { it != id }) args += listOf("--peer", "$peer=127.0.0.1:${ports.getValue(peer)}")
                processes[id] = ProcessBuilder(args).redirectErrorStream(true)
                    .redirectOutput(dir.resolve("$id.log").toFile()).start()
            }
            eventually {
                check(processes.values.all { it.isAlive }) { "Una JVM electoral terminó antes de elegir" }
                leaders().isNotEmpty()
            }
            val initial = leaders().single()
            Thread.sleep(850)
            check(leaders() == listOf(initial)) { "Los heartbeats no conservaron al líder inicial" }
            val old = processes.getValue(initial.first)
            check(old.destroyForcibly().waitFor(5, TimeUnit.SECONDS))
            eventually { leaders(initial.first).any { it.second > initial.second } }
            val replacement = leaders(initial.first).single { it.second > initial.second }
            println("Tres JVM por TCP: líder inicial ${initial.first} término ${initial.second}; " +
                "tras la caída, ${replacement.first} término ${replacement.second}; commitIndex=0")
        } finally {
            processes.values.filter { it.isAlive }.forEach { it.destroy() }
            processes.values.forEach {
                if (!it.waitFor(5, TimeUnit.SECONDS)) check(it.destroyForcibly().waitFor(5, TimeUnit.SECONDS))
            }
        }
    }

    @JvmStatic
    fun main(args: Array<String>) {
        scenario("timeout, voto propio, respuesta inválida, reintento y heartbeat DM2 por TCP", ::electionAndRetry)
        scenario("votación dividida, duplicado, emisor desconocido y reelección", ::duplicateAndUnknownDoNotWin)
        scenario("respuesta de otra elección por TCP", { delayedReply(it, 1) })
        scenario("término superior tardío por TCP", { delayedReply(it, 9) })
        scenario("heartbeats incompatibles, expiración, comparación del log y voto conservado", ::heartbeatAndCommit)
        scenario("rechazo de CLIENT_COMMAND por un líder experimental", ::commandRejection)
        scenario("un RPC por réplica y cierre de socket ocupado", ::boundAndClose)
        scenario("agotamiento de término sin desbordamiento", ::exhaustedTerm)
        scenario("tres JVM y nuevo líder tras perder heartbeats", ::processFailover)
        println("PASS: 9/9 escenarios electorales Kotlin")
    }
}
