import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.TimeUnit

object RaftStorageTest {
    private val loopback = InetAddress.getByName("127.0.0.1")
    private val requestId = UUID(0, 1)
    private val peers = mapOf("c1" to InetSocketAddress(loopback, 19111),
        "c2" to InetSocketAddress(loopback, 19112))

    private inline fun <reified T : Throwable> rejects(block: () -> Unit) {
        val error = runCatching(block).exceptionOrNull()
        check(error is T) { "Se esperaba ${T::class.simpleName}, se obtuvo $error" }
    }

    private fun scenario(name: String, test: (Path) -> Unit) {
        val directory = Files.createTempDirectory("dog raft kotlin ")
        try {
            test(directory)
            println("PASS $name")
        } finally {
            Files.walk(directory).use { paths ->
                paths.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) }
            }
        }
    }

    private fun command(index: Long, bytes: ByteArray) = EntradaLog(index, 3, "c1", requestId, bytes)

    private fun logRecovery(dir: Path) {
        val bytes = byteArrayOf(0, -1, -128)
        val entry = command(1, bytes)
        bytes[0] = 42
        entry.command[1] = 42
        RaftStorage(dir, "n3").use { storage ->
            check(storage.snapshot() == RaftState())
            check(Files.isRegularFile(dir.resolve("raft-state.bin")))
            val log = mutableListOf(entry, EntradaLog(2, 4), command(1, byteArrayOf(1)))
            rejects<IllegalArgumentException> { storage.save(RaftState(4, "c1", 2, log)) }
            log[2] = EntradaLog(3, 4, "c1", requestId, byteArrayOf(0, -1, -128))
            log.add(EntradaLog(4, 4, "c2", requestId, byteArrayOf(42)))
            storage.save(RaftState(4, "c1", 3, log))
            log.clear()
            check(storage.snapshot().log.size == 4)
        }
        RaftStorage(dir, "n3").use { storage ->
            val state = storage.snapshot()
            check(state.currentTerm == 4L && state.votedFor == "c1" && state.commitIndex == 3L)
            check(state.log[0].command.contentEquals(byteArrayOf(0, -1, -128)))
            check(state.log[1].isNoOp)
            val inspection = PersistentRaftNode("n3", peers, storage).inspect()
            check("lastApplied=3 lastLogIndex=4 appliedCommands=[1]" in inspection)
            check("type=NO_OP status=COMMITTED" in inspection && "command=00ff80" in inspection)
            check("index=4 term=4 type=COMMAND status=PENDING" in inspection)
        }
        rejects<IOException> { RaftStorage(dir, "otro").close() }
    }

    private fun protectedState(dir: Path) {
        RaftStorage(dir, "n3").use { storage ->
            val initial = RaftState(3, "c1", 1, listOf(command(1, byteArrayOf(1)), EntradaLog(2, 3)))
            storage.save(initial)
            rejects<IllegalArgumentException> { storage.save(initial.copy(votedFor = "c2")) }
            rejects<IllegalArgumentException> { storage.save(initial.copy(votedFor = null)) }
            rejects<IllegalArgumentException> { storage.save(initial.copy(commitIndex = 0)) }
            rejects<IllegalArgumentException> { storage.save(initial.copy(commitIndex = 3)) }
            rejects<IllegalArgumentException> {
                storage.save(initial.copy(log = listOf(command(1, byteArrayOf(2)))))
            }
            storage.save(initial.copy(currentTerm = 4, votedFor = null, log = initial.log.take(1)))
        }
        RaftStorage(dir, "n3").use { storage ->
            check(storage.snapshot().log.single().command.contentEquals(byteArrayOf(1)))
            check(storage.snapshot().currentTerm == 4L && storage.snapshot().votedFor == null)
        }
    }

    private fun damagedState(dir: Path) {
        RaftStorage(dir, "n3").use { it.save(RaftState(4, "c1")) }
        val file = dir.resolve("raft-state.bin")
        val valid = Files.readAllBytes(file)
        val corrupt = valid.copyOf().apply { this[lastIndex] = (last().toInt() xor 1).toByte() }
        for (damaged in listOf(valid.copyOf(valid.size - 1), corrupt, valid + byteArrayOf(1))) {
            Files.write(file, damaged)
            rejects<IOException> { RaftStorage(dir, "n3").close() }
            check(Files.readAllBytes(file).contentEquals(damaged))
        }
        Files.write(file, valid)
        RaftStorage(dir, "n3").use { check(it.snapshot().votedFor == "c1") }
    }

    private fun failedWrite(dir: Path) {
        val file = dir.resolve("raft-state.bin")
        val backup = dir.resolve("respaldo")
        RaftStorage(dir, "n3").use { storage ->
            val node = PersistentRaftNode("n3", peers, storage)
            check(node.requestVote("c1", 3, 0, 0).granted)
            Files.move(file, backup)
            Files.createDirectory(file)
            // El destino no es reemplazable por un archivo. El guardado falla tras escribir el temporal.
            Files.writeString(file.resolve("bloqueo"), "ocupado")
            val vote = ByteArrayOutputStream()
            DataOutputStream(vote).use {
                Dm2Protocol.escribirHeader(it, Dm2Protocol.TYPE_RAFT_REQUEST_VOTE, byteArrayOf(1) + ByteArray(15), "c2")
                it.writeLong(4)
                it.writeLong(0)
                it.writeLong(0)
            }
            val response = ByteArrayOutputStream()
            rejects<IOException> { node.handleDm2(vote.toByteArray(), DataOutputStream(response)) }
            check(response.size() == 0) { "Se emitieron bytes de respuesta pese al fallo" }
            rejects<IOException> { node.requestVote("c1", 3, 0, 0) }
            rejects<IOException> { storage.snapshot() }
            Files.list(dir).use { files -> check(files.noneMatch { it.toString().endsWith(".tmp") }) }
        }
        Files.delete(file.resolve("bloqueo"))
        Files.delete(file)
        Files.move(backup, file)
        RaftStorage(dir, "n3").use { check(it.snapshot() == RaftState(3, "c1")) }
    }

    private fun commandLine(dir: Path, args: List<String>): ProcessBuilder {
        val java = Path.of(System.getProperty("java.home"), "bin", "java").toString()
        val classpath = System.getProperty("java.class.path")
        return ProcessBuilder(listOf(java, "-cp", classpath, "NodeMainKt", "--id", "n3",
            "--data-dir", dir.toString()) + args).redirectErrorStream(true)
    }

    private fun inspect(dir: Path): Pair<Int, String> {
        val process = commandLine(dir, listOf("--inspect")).start()
        try {
            check(process.waitFor(8, TimeUnit.SECONDS)) { "Timeout de --inspect" }
            return process.exitValue() to process.inputStream.bufferedReader().use { it.readText() }
        } finally {
            if (process.isAlive) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS)
        }
    }

    private fun exchange(port: Int, peer: String, type: Int, term: Long): Triple<Long, Boolean, Int>? {
        Socket(loopback, port).use { socket ->
            socket.soTimeout = 3000
            val output = DataOutputStream(socket.getOutputStream())
            output.writeInt(23 + peer.length + if (type == Dm2Protocol.TYPE_RAFT_REQUEST_VOTE) 24 else 34)
            Dm2Protocol.escribirHeader(output, type, byteArrayOf(1) + ByteArray(15), peer)
            output.writeLong(term)
            val voting = type == Dm2Protocol.TYPE_RAFT_REQUEST_VOTE
            output.writeLong(if (voting) 2 else 0)
            output.writeLong(if (voting) 3 else 0)
            if (type == Dm2Protocol.TYPE_RAFT_APPEND_ENTRIES) {
                output.writeLong(0)
                output.writeShort(0)
            }
            output.flush()
            val input = DataInputStream(socket.getInputStream())
            val first = input.read()
            if (first == -1) return null
            check(first == 0 && input.readUnsignedByte() == 0)
            val body = ByteArray(input.readUnsignedShort())
            input.readFully(body)
            val header = Dm2Protocol.leerHeader(body)
            check(header.senderId == "n3" && header.requestId.contentEquals(byteArrayOf(1) + ByteArray(15)))
            val payload = DataInputStream(body.inputStream())
            payload.skipNBytes(25)
            if (header.type == Dm2Protocol.TYPE_ERROR) return Triple(payload.readUnsignedByte().toLong(), false, header.type)
            return Triple(payload.readLong(), payload.readBoolean(), header.type)
        }
    }

    private fun processRecovery(dir: Path) {
        RaftStorage(dir, "n3").use {
            it.save(RaftState(3, null, 2, listOf(command(1, byteArrayOf(0, -1, -128)), EntradaLog(2, 3))))
        }
        val port = ServerSocket(0, 1, loopback).use { it.localPort }
        val log = dir.resolve("proceso.log")
        val child = commandLine(dir, listOf("--host", "127.0.0.1", "--port", port.toString(), "--local-raft",
            "--peer", "c1=127.0.0.1:19111", "--peer", "c2=127.0.0.1:19112"))
            .redirectOutput(log.toFile()).start()
        try {
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8)
            while (child.isAlive && "LISTEN" !in Files.readString(log) && System.nanoTime() < deadline) {
                Thread.sleep(30)
            }
            check(child.isAlive && "LISTEN" in Files.readString(log)) { Files.readString(log) }
            val voteType = Dm2Protocol.TYPE_RAFT_REQUEST_VOTE
            val responseType = Dm2Protocol.TYPE_RAFT_REQUEST_VOTE_RESPONSE
            check(exchange(port, "c1", voteType, 7) == Triple(7L, true, responseType))
            check(exchange(port, "c2", Dm2Protocol.TYPE_RAFT_APPEND_ENTRIES, 7) ==
                Triple(7L, true, Dm2Protocol.TYPE_RAFT_APPEND_ENTRIES_RESPONSE))
            check(exchange(port, "c2", voteType, 7) == Triple(7L, false, responseType))
            check(exchange(port, "intruso", voteType, 99) == Triple(4L, false, Dm2Protocol.TYPE_ERROR))
            val locked = inspect(dir)
            check(locked.first == 1 && "Directorio Raft en uso" in locked.second) { locked.second }
        } finally {
            check(child.destroyForcibly().waitFor(5, TimeUnit.SECONDS))
        }
        val recovered = inspect(dir)
        check(recovered.first == 0 && "term=7 votedFor=c1" in recovered.second &&
            "commitIndex=2 lastApplied=2 lastLogIndex=2 appliedCommands=[1]" in recovered.second &&
            "command=00ff80" in recovered.second) { recovered.second }
        println("Tras terminar el proceso: ${recovered.second.trim()}")
        RaftStorage(dir, "n3").use { storage ->
            val node = PersistentRaftNode("n3", peers, storage)
            check(!node.requestVote("c2", 7, 2, 3).granted)
            check(node.requestVote("c2", 8, 2, 3).granted)
        }
    }

    @JvmStatic
    fun main(args: Array<String>) {
        scenario("provisión, rutas con espacios, log binario, commit, NO_OP y recuperación", ::logRecovery)
        scenario("protección de voto, índice comprometido y prefijo", ::protectedState)
        scenario("rechazo de truncado, CRC incorrecto e información sobrante", ::damagedState)
        scenario("fallo de reemplazo y detención sin conceder el voto", ::failedWrite)
        scenario("bloqueo en el mismo JVM y reapertura", { dir ->
            RaftStorage(dir, "n3").use {
                rejects<IOException> { RaftStorage(dir, "n3").close() }
            }
            RaftStorage(dir, "n3").close()
        })
        scenario("exclusión entre procesos y recuperación tras terminación forzada", ::processRecovery)
        println("PASS: 6/6 escenarios Kotlin")
    }
}
