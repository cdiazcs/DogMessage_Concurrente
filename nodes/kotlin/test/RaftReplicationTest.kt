import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread

object RaftReplicationTest {
    private val local = InetAddress.getByName("127.0.0.1")
    private val binary = byteArrayOf(0, -1, -128, 42)
    private val token = UUID(0, 7)
    private val loopbackPeer = mapOf("n1" to InetSocketAddress(local, 19191))

    private inline fun <reified T : Throwable> rejects(block: () -> Unit) {
        val error = runCatching(block).exceptionOrNull()
        check(error is T) { "Se esperaba ${T::class.simpleName}, se obtuvo $error" }
    }

    private fun scenario(label: String, body: (Path) -> Unit) {
        val path = Files.createTempDirectory("kotlin replication ")
        try {
            body(path)
            println("PASS $label")
        } finally {
            Files.walk(path).use { files -> files.sorted(Comparator.reverseOrder()).forEach(Files::deleteIfExists) }
        }
    }

    private fun frame(request: Dm2Protocol.Request, sender: String, id: ByteArray): ByteArray {
        val bytes = ByteArrayOutputStream()
        Dm2Protocol.writeRequest(DataOutputStream(bytes), sender, id, request)
        return bytes.toByteArray().copyOfRange(4, bytes.size())
    }

    private fun client(node: PersistentRaftNode, id: ByteArray, command: ByteArray = binary): Triple<Int, Long, String> {
        val bytes = ByteArrayOutputStream()
        node.handleDm2(frame(Dm2Protocol.ClientCommand(command), "cliente", id), DataOutputStream(bytes))
        val wire = DataInputStream(bytes.toByteArray().inputStream())
        val body = wire.readNBytes(wire.readInt())
        val header = Dm2Protocol.leerHeader(body)
        check(header.requestId.contentEquals(id) && header.type == Dm2Protocol.TYPE_CLIENT_RESULT)
        val payload = DataInputStream(body.inputStream())
        payload.skipNBytes((23 + header.senderId.length).toLong())
        return Triple(payload.readUnsignedByte(), payload.readLong(),
            String(payload.readNBytes(payload.readUnsignedByte()), Charsets.US_ASCII))
    }

    private fun mergeAndRecover(dir: Path) {
        RaftStorage(dir, "n3").use { storage ->
            PersistentRaftNode("n3", loopbackPeer, storage, replication = true).use { node ->
                val command = EntradaLog(1, 1, "cliente", token, binary)
                val first = Dm2Protocol.Append(2, 0, 0, 1, listOf(command))
                check(node.appendEntries("n1", first) == Dm2Protocol.Reply(2, true, 1))
                val suffix = listOf(EntradaLog(2, 2), EntradaLog(3, 2, "cliente", UUID(0, 8), byteArrayOf(5)))
                val second = Dm2Protocol.Append(2, 1, 1, 1, suffix)
                repeat(2) { check(node.appendEntries("n1", second) == Dm2Protocol.Reply(2, true, 3)) }
                check(!node.appendEntries("n1", Dm2Protocol.Append(2, 4, 2, 3)).success)
                check(!node.appendEntries("n1", Dm2Protocol.Append(3, 3, 1, 3)).success)
                check(!node.appendEntries("n1", second).success)
                check(storage.snapshot().log.size == 3 && storage.snapshot().commitIndex == 1L)
                check("lastApplied=1" in node.inspect())
                val replacement = EntradaLog(2, 3, "cliente", UUID(0, 9), byteArrayOf(9))
                check(node.appendEntries("n1", Dm2Protocol.Append(3, 1, 1, 2, listOf(replacement))).success)
                check(client(node, Dm2Protocol.generarRequestId()) == Triple(1, 0L, "n1"))
            }
        }
        RaftStorage(dir, "n3").use { storage ->
            val value = storage.snapshot()
            check(value.currentTerm == 3L && value.commitIndex == 2L && value.log.size == 2)
            check(value.log[0].hasCommand(binary) && value.log[1].hasCommand(byteArrayOf(9)))
            PersistentRaftNode("n3", loopbackPeer, storage, replication = true).use {
                check("lastApplied=2 lastLogIndex=2 appliedCommands=[1, 2]" in it.inspect())
            }
        }
    }

    private fun protectPrefix(dir: Path, sameTerm: Boolean) {
        RaftStorage(dir, "n3").use { storage ->
            val entry = EntradaLog(1, 1, "cliente", token, binary)
            val original = if (sameTerm) listOf(entry, EntradaLog(2, 3), EntradaLog(3, 5)) else listOf(entry)
            storage.save(RaftState(5, null, 1, original))
            PersistentRaftNode("n3", loopbackPeer, storage, replication = true).use { node ->
                val append = if (sameTerm) {
                    val forbidden = EntradaLog(3, 5, "cliente", token, byteArrayOf(99))
                    Dm2Protocol.Append(5, 1, 1, 1, listOf(EntradaLog(2, 4), forbidden))
                } else {
                    Dm2Protocol.Append(5, 0, 0, 1, listOf(EntradaLog(1, 2)))
                }
                val wire = frame(append, "n1", Dm2Protocol.generarRequestId())
                val ack = ByteArrayOutputStream()
                rejects<IOException> { node.handleDm2(wire, DataOutputStream(ack)) }
                check(ack.size() == 0 && storage.snapshot().log.zip(original).all { (a, b) -> a.sameEntry(b) })
                check(storage.snapshot().log.size == original.size)
                rejects<IOException> { node.appendEntries("n1", Dm2Protocol.Append(5, 1, 1, 1)) }
            }
        }
    }

    private fun failedWrite(dir: Path) {
        val stateFile = dir.resolve("raft-state.bin")
        RaftStorage(dir, "n3").use { storage ->
            storage.save(RaftState(1))
            PersistentRaftNode("n3", loopbackPeer, storage, replication = true).use { node ->
                Files.move(stateFile, dir.resolve("backup"))
                Files.createDirectory(stateFile)
                val ack = ByteArrayOutputStream()
                val append = Dm2Protocol.Append(1, 0, 0, 1, listOf(EntradaLog(1, 1)))
                rejects<IOException> {
                    node.handleDm2(frame(append, "n1", Dm2Protocol.generarRequestId()), DataOutputStream(ack))
                }
                check(ack.size() == 0)
                rejects<IOException> { client(node, Dm2Protocol.generarRequestId()) }
            }
        }
        Files.delete(stateFile)
        Files.move(dir.resolve("backup"), stateFile)
        RaftStorage(dir, "n3").use { check(it.snapshot() == RaftState(1)) }
    }

    private fun failedClientWrite(dir: Path) {
        val stateFile = dir.resolve("raft-state.bin")
        RaftStorage(dir, "n3").use { storage ->
            val time = AtomicLong()
            PersistentRaftNode("n3", emptyMap(), storage, { TimeUnit.MILLISECONDS.toNanos(time.get()) },
                replication = true) { 500 }.use { node ->
                time.set(500)
                node.tick()
                check(storage.snapshot().commitIndex == 1L)
                Files.move(stateFile, dir.resolve("backup"))
                Files.createDirectory(stateFile)
                val ack = ByteArrayOutputStream()
                rejects<IOException> {
                    val command = frame(Dm2Protocol.ClientCommand(binary), "cliente", Dm2Protocol.generarRequestId())
                    node.handleDm2(command, DataOutputStream(ack))
                }
                check(ack.size() == 0)
            }
        }
        Files.delete(stateFile)
        Files.move(dir.resolve("backup"), stateFile)
        RaftStorage(dir, "n3").use {
            check(it.snapshot().commitIndex == 1L && it.snapshot().log.single().isNoOp)
        }
    }

    private fun quorumOfCurrentTerm(dir: Path) {
        ServerSocket(0, 10, local).use { peer ->
            val pendingNoOp = CountDownLatch(1)
            val release = CountDownLatch(1)
            val invalidSent = AtomicBoolean()
            val remoteIndex = AtomicLong()
            val failure = AtomicReference<Throwable>()
            val server = thread(isDaemon = true) {
                try {
                    while (!peer.isClosed) peer.accept().use { socket ->
                        val body = Dm2Protocol.readFrame(socket, 1000)
                        val header = Dm2Protocol.leerHeader(body)
                        val request = Dm2Protocol.readRequest(body, header)
                        val out = DataOutputStream(socket.getOutputStream())
                        Dm2Protocol.writeFrame(out, "n1", header.requestId, request.type or 0x80) {
                            when (request) {
                                is Dm2Protocol.Vote -> {
                                    it.writeLong(request.term)
                                    it.writeBoolean(true)
                                }
                                is Dm2Protocol.Append -> {
                                    val matches = request.index <= remoteIndex.get()
                                    it.writeLong(request.term)
                                    it.writeBoolean(matches)
                                    var index = 0L
                                    if (matches) {
                                        if (request.lastIndex == 257L && !invalidSent.getAndSet(true)) {
                                            // Un éxito con matchIndex inventado debe descartarse.
                                            index = 258
                                        } else {
                                            if (request.lastIndex == 257L) {
                                                pendingNoOp.countDown()
                                                check(release.await(2, TimeUnit.SECONDS))
                                            }
                                            index = request.lastIndex
                                            remoteIndex.set(maxOf(remoteIndex.get(), index))
                                        }
                                    }
                                    it.writeLong(index)
                                }
                                else -> error("Solicitud inesperada")
                            }
                        }
                    }
                } catch (error: Exception) {
                    if (!peer.isClosed) failure.set(error)
                }
            }
            try {
                val time = AtomicLong()
                val peers = mapOf("n1" to InetSocketAddress(local, peer.localPort), "n2" to InetSocketAddress(local, 1))
                RaftStorage(dir, "n3").use { storage ->
                    storage.save(RaftState(4, null, 0, List(256) { EntradaLog(it + 1L, 4) }))
                    PersistentRaftNode("n3", peers, storage, { TimeUnit.MILLISECONDS.toNanos(time.get()) },
                        replication = true) { 500 }.use { node ->
                        time.set(500)
                        node.tick()
                        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(6)
                        try {
                            while (!pendingNoOp.await(10, TimeUnit.MILLISECONDS)) {
                                check(System.nanoTime() < deadline) { "No avanzó el prefijo común" }
                                time.addAndGet(125)
                                node.tick()
                            }
                            check(remoteIndex.get() == 256L && invalidSent.get())
                            check(storage.snapshot().commitIndex == 0L)
                        } finally {
                            release.countDown()
                        }
                        while (storage.snapshot().commitIndex != 257L) {
                            check(System.nanoTime() < deadline) { "El NO_OP vigente no se comprometió" }
                            Thread.sleep(5)
                        }
                        check("lastApplied=257" in node.inspect())
                    }
                }
            } finally {
                release.countDown()
                peer.close()
                server.join(2500)
            }
            failure.get()?.let { throw AssertionError("Falló el peer controlado", it) }
        }
    }

    private fun clientsAndRecovery(dir: Path) {
        val id = Dm2Protocol.generarRequestId()
        for (expectedTerm in 1L..2L) {
            RaftStorage(dir, "n3").use { storage ->
                val time = AtomicLong()
                PersistentRaftNode("n3", emptyMap(), storage, { TimeUnit.MILLISECONDS.toNanos(time.get()) },
                    replication = true) { 500 }.use { node ->
                    time.set(500)
                    node.tick()
                    check(client(node, id) == Triple(0, 2L, ""))
                    check(client(node, id) == Triple(0, 2L, ""))
                    check(client(node, id, byteArrayOf(42)) == Triple(3, 0L, ""))
                    check(storage.snapshot().log.count { !it.isNoOp } == 1)
                    check(storage.snapshot().currentTerm == expectedTerm)
                }
            }
        }
    }

    private fun clientLosesLeadership(dir: Path) {
        // El banco de tres JVM comprueba el timeout sin mayoría. Aquí se fuerza una transición
        // durante la espera de un comando ya persistido y todavía sin compromiso.
        ServerSocket(0, 5, local).use { peer ->
            val appendStarted = CountDownLatch(1)
            val release = CountDownLatch(1)
            val oldReplySent = CountDownLatch(1)
            val peerThread = thread(isDaemon = true) {
                try {
                    while (!peer.isClosed) peer.accept().use { socket ->
                        val body = Dm2Protocol.readFrame(socket, 1000)
                        val header = Dm2Protocol.leerHeader(body)
                        when (val request = Dm2Protocol.readRequest(body, header)) {
                            is Dm2Protocol.Vote -> Dm2Protocol.writeFrame(
                                DataOutputStream(socket.getOutputStream()), "n1", header.requestId,
                                Dm2Protocol.TYPE_RAFT_REQUEST_VOTE_RESPONSE) {
                                it.writeLong(request.term)
                                it.writeBoolean(true)
                            }
                            is Dm2Protocol.Append -> {
                                appendStarted.countDown()
                                check(release.await(2, TimeUnit.SECONDS))
                                Dm2Protocol.writeFrame(DataOutputStream(socket.getOutputStream()), "n1", header.requestId,
                                    Dm2Protocol.TYPE_RAFT_APPEND_ENTRIES_RESPONSE) {
                                    it.writeLong(request.term)
                                    it.writeBoolean(true)
                                    it.writeLong(request.lastIndex)
                                }
                                oldReplySent.countDown()
                            }
                            else -> error("Solicitud inesperada")
                        }
                    }
                } catch (_: IOException) {
                    check(peer.isClosed)
                }
            }
            try {
                RaftStorage(dir, "n3").use { storage ->
                    val time = AtomicLong()
                    val peers = mapOf("n1" to InetSocketAddress(local, peer.localPort))
                    PersistentRaftNode("n3", peers, storage, { TimeUnit.MILLISECONDS.toNanos(time.get()) },
                        replication = true) { 500 }.use { node ->
                        time.set(500)
                        node.tick()
                        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3)
                        while (!node.inspect().startsWith("role=LEADER")) {
                            check(System.nanoTime() < deadline)
                            Thread.sleep(5)
                        }
                        check(appendStarted.await(2, TimeUnit.SECONDS))
                        val response = AtomicReference<Triple<Int, Long, String>>()
                        val waiter = thread { response.set(client(node, Dm2Protocol.generarRequestId())) }
                        try {
                            while (storage.snapshot().log.size < 2) {
                                check(System.nanoTime() < deadline)
                                Thread.sleep(5)
                            }
                            node.appendEntries("n1", Dm2Protocol.Append(2, 0, 0, 0))
                            release.countDown()
                            check(oldReplySent.await(2, TimeUnit.SECONDS))
                            waiter.join(2000)
                            check(response.get() == Triple(2, 0L, ""))
                            check(storage.snapshot().commitIndex == 0L)
                            check(node.inspect().startsWith("role=FOLLOWER term=2"))
                        } finally {
                            node.close()
                            waiter.join(2000)
                        }
                    }
                }
            } finally {
                release.countDown()
                peer.close()
                peerThread.join(2000)
            }
        }
    }

    @JvmStatic
    fun main(args: Array<String>) {
        scenario("lotes idempotentes, rechazo de posición y reparación de sufijo tras reinicio", ::mergeAndRecover)
        scenario("protección del prefijo comprometido", { protectPrefix(it, false) })
        scenario("detección de contenido distinto para mismo índice y término", { protectPrefix(it, true) })
        scenario("fallo de persistencia sin ACK de replicación", ::failedWrite)
        scenario("fallo de escritura local sin confirmar CLIENT_COMMAND", ::failedClientWrite)
        scenario("quórum no compromete prefijo antiguo hasta confirmar el NO_OP y descarta matchIndex falso", ::quorumOfCurrentTerm)
        scenario("CLIENT_COMMAND, deduplicación y conflicto tras reinicio", ::clientsAndRecovery)
        scenario("RETRY al perder liderazgo con cliente esperando", ::clientLosesLeadership)
        println("PASS: 8/8 escenarios de replicación y comandos Kotlin")
    }
}
