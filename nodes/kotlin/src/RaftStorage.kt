import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.File
import java.io.IOException
import java.nio.channels.Channels
import java.nio.channels.FileChannel
import java.nio.channels.OverlappingFileLockException
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardCopyOption.ATOMIC_MOVE
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.READ
import java.nio.file.StandardOpenOption.WRITE
import java.util.UUID
import java.util.zip.CRC32C
import java.util.zip.CheckedInputStream
import java.util.zip.CheckedOutputStream

internal val raftIdPattern = Regex("[A-Za-z0-9_-]{1,32}")
private const val MAX_COMMAND_BYTES = 65_536
private const val STORAGE_MAGIC = 0x444B5231 // DKR1

internal class EntradaLog(
    val index: Long,
    val term: Long,
    val clientId: String? = null,
    val requestId: UUID? = null,
    command: ByteArray = byteArrayOf()
) {
    private val bytes = command.copyOf()
    val isNoOp: Boolean get() = clientId == null
    val command: ByteArray get() = bytes.copyOf()

    init {
        require(index > 0 && term > 0) { "Índice o término de entrada inválido" }
        if (isNoOp) {
            require(requestId == null && bytes.isEmpty()) { "NO_OP no lleva comando" }
        } else {
            require(raftIdPattern.matches(checkNotNull(clientId))) { "ID de cliente inválido" }
            require(requestId != null && requestId != UUID(0, 0)) { "ID de solicitud inválido" }
            require(bytes.size in 1..MAX_COMMAND_BYTES) { "Longitud de comando inválida" }
        }
    }

    fun sameEntry(other: EntradaLog): Boolean = index == other.index && term == other.term &&
        clientId == other.clientId && requestId == other.requestId && bytes.contentEquals(other.bytes)

    fun hasCommand(other: ByteArray): Boolean = bytes.contentEquals(other)

    val dm2Size: Int get() = 9 + if (clientId == null) 0 else 1 + clientId.length + 16 + 4 + bytes.size

    fun writeDm2(out: DataOutputStream) {
        out.writeLong(term)
        out.writeByte(if (isNoOp) 0 else 1)
        if (!isNoOp) {
            val client = checkNotNull(clientId)
            out.writeByte(client.length)
            out.write(client.toByteArray(Charsets.US_ASCII))
            writeCommand(out)
        }
    }

    private fun writeCommand(out: DataOutputStream) {
        val token = checkNotNull(requestId)
        out.writeLong(token.mostSignificantBits)
        out.writeLong(token.leastSignificantBits)
        out.writeInt(bytes.size)
        out.write(bytes)
    }

    fun writeTo(out: DataOutputStream) {
        out.writeLong(index)
        out.writeLong(term)
        out.writeByte(if (isNoOp) 0 else 1)
        if (!isNoOp) {
            out.writeUTF(checkNotNull(clientId))
            writeCommand(out)
        }
    }

    companion object {
        fun readFrom(input: DataInputStream): EntradaLog {
            val index = input.readLong()
            val term = input.readLong()
            return when (input.readUnsignedByte()) {
                0 -> EntradaLog(index, term)
                1 -> {
                    val client = input.readUTF()
                    val request = UUID(input.readLong(), input.readLong())
                    val length = input.readInt()
                    require(length in 1..MAX_COMMAND_BYTES) { "Longitud de comando inválida" }
                    val command = ByteArray(length)
                    input.readFully(command)
                    EntradaLog(index, term, client, request, command)
                }
                else -> throw IOException("Tipo de entrada desconocido")
            }
        }
    }
}

internal data class RaftState(
    val currentTerm: Long = 0,
    val votedFor: String? = null,
    val commitIndex: Long = 0,
    val log: List<EntradaLog> = emptyList()
)

/** Una instantánea completa y un bloqueo exclusivo durante toda la vida del motor. */
internal class RaftStorage(directory: Path, private val nodeId: String) : AutoCloseable {
    private val directory = directory.toAbsolutePath().normalize()
    private val stateFile = this.directory.resolve("raft-state.bin")
    private val lockChannel: FileChannel
    private var state: RaftState
    private var failed = false

    init {
        require(raftIdPattern.matches(nodeId)) { "ID de nodo inválido" }
        Files.createDirectories(this.directory)
        lockChannel = FileChannel.open(this.directory.resolve("kotlin-raft.lock"), CREATE, WRITE)
        try {
            val lock = try {
                lockChannel.tryLock()
            } catch (_: OverlappingFileLockException) {
                null
            }
            if (lock == null) throw IOException("Directorio Raft en uso: ${this.directory}")
            state = readState() ?: RaftState().also(::writeState)
        } catch (error: Exception) {
            try {
                lockChannel.close()
            } catch (closeError: IOException) {
                error.addSuppressed(closeError)
            }
            throw error
        }
    }

    @Synchronized
    fun snapshot(): RaftState {
        ensureAvailable()
        return state
    }

    @Synchronized
    fun save(next: RaftState) {
        ensureAvailable()
        val candidate = next.copy(log = java.util.List.copyOf(next.log))
        validate(candidate)
        require(candidate.currentTerm >= state.currentTerm) { "El término no puede retroceder" }
        require(candidate.currentTerm != state.currentTerm || state.votedFor == null ||
            candidate.votedFor == state.votedFor) { "El voto no puede cambiar dentro del mismo término" }
        require(candidate.commitIndex >= state.commitIndex) { "commitIndex no puede retroceder" }
        for (i in 0 until state.commitIndex.toInt()) {
            require(state.log[i].sameEntry(candidate.log[i])) { "Conflicto con el prefijo comprometido" }
        }
        try {
            writeState(candidate)
            state = candidate
        } catch (error: IOException) {
            failed = true
            throw error
        }
    }

    private fun ensureAvailable() {
        if (failed || !lockChannel.isOpen) throw IOException("Almacenamiento Raft detenido o cerrado")
    }

    private fun validate(value: RaftState) {
        require(value.currentTerm >= 0 && value.commitIndex in 0..value.log.size.toLong()) {
            "Término o commitIndex inválido"
        }
        require(value.votedFor == null || (value.currentTerm > 0 && raftIdPattern.matches(value.votedFor))) {
            "Voto inválido"
        }
        var previous = 0L
        for ((i, entry) in value.log.withIndex()) {
            require(entry.index == i + 1L && entry.term in previous..value.currentTerm) {
                "Secuencia de índices o términos inválida"
            }
            previous = entry.term
        }
    }

    private fun readState(): RaftState? {
        val stream = try {
            Files.newInputStream(stateFile).buffered()
        } catch (_: NoSuchFileException) {
            return null
        }
        try {
            stream.use { raw ->
                val checked = CheckedInputStream(raw, CRC32C())
                val input = DataInputStream(checked)
                require(input.readInt() == STORAGE_MAGIC && input.readUTF() == nodeId) {
                    "Formato o identidad de nodo incompatible"
                }
                val term = input.readLong()
                val vote = input.readUTF().ifEmpty { null }
                val commit = input.readLong()
                val count = input.readInt()
                require(count >= 0 && count.toLong() <= Files.size(stateFile) / 17) {
                    "Cantidad de entradas inválida"
                }
                val entries = ArrayList<EntradaLog>()
                repeat(count) { entries.add(EntradaLog.readFrom(input)) }
                val checksum = DataInputStream(raw).readInt()
                require(checksum == checked.checksum.value.toInt() && raw.read() == -1) {
                    "Checksum inválido o bytes sobrantes"
                }
                return RaftState(term, vote, commit, java.util.List.copyOf(entries)).also(::validate)
            }
        } catch (error: Exception) {
            throw IOException("No se pudo recuperar $stateFile: ${error.message ?: "estado truncado"}", error)
        }
    }

    private fun writeState(value: RaftState) {
        val temporary = Files.createTempFile(directory, "raft-state-", ".tmp")
        try {
            FileChannel.open(temporary, WRITE).use { channel ->
                val raw = Channels.newOutputStream(channel).buffered()
                val checked = CheckedOutputStream(raw, CRC32C())
                val out = DataOutputStream(checked)
                out.writeInt(STORAGE_MAGIC)
                out.writeUTF(nodeId)
                out.writeLong(value.currentTerm)
                out.writeUTF(value.votedFor ?: "")
                out.writeLong(value.commitIndex)
                out.writeInt(value.log.size)
                value.log.forEach { it.writeTo(out) }
                DataOutputStream(raw).writeInt(checked.checksum.value.toInt())
                raw.flush()
                channel.force(true)
            }
            Files.move(temporary, stateFile, ATOMIC_MOVE, REPLACE_EXISTING)
            if (File.separatorChar != '\\') {
                FileChannel.open(directory, READ).use { it.force(true) }
            }
        } catch (error: Exception) {
            try {
                Files.deleteIfExists(temporary)
            } catch (cleanupError: IOException) {
                error.addSuppressed(cleanupError)
            }
            throw error
        }
    }

    @Synchronized
    override fun close() = lockChannel.close()
}
