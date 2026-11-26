import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.IOException
import java.net.Socket
import java.net.SocketTimeoutException
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.TimeUnit

internal object Dm2Protocol {
    val MAGIC = byteArrayOf(0x44, 0x4D, 0x32, 0x00)
    const val ALGORITHM_RAFT = 1
    const val TYPE_RAFT_REQUEST_VOTE = 0x01
    const val TYPE_RAFT_APPEND_ENTRIES = 0x02
    const val TYPE_CLIENT_COMMAND = 0x03
    const val TYPE_RAFT_REQUEST_VOTE_RESPONSE = 0x81
    const val TYPE_RAFT_APPEND_ENTRIES_RESPONSE = 0x82
    const val TYPE_CLIENT_RESULT = 0x83
    const val TYPE_ERROR = 0xff
    const val MAX_FRAME = 1_048_576
    private const val HEADER_BYTES = 23
    private const val APPEND_FIXED_BYTES = 34
    const val MAX_APPEND_ENTRIES = 256

    data class Header(val algorithm: Int, val type: Int, val requestId: ByteArray, val senderId: String)
    sealed interface Request { val type: Int }
    data class Vote(val term: Long, val index: Long, val logTerm: Long) : Request {
        override val type = TYPE_RAFT_REQUEST_VOTE
    }
    data class Append(
        val term: Long, val index: Long, val logTerm: Long, val commit: Long, val entries: List<EntradaLog> = emptyList()
    ) : Request {
        override val type = TYPE_RAFT_APPEND_ENTRIES
        val lastIndex: Long get() = index + entries.size
    }
    data class ClientCommand(val bytes: ByteArray) : Request { override val type = TYPE_CLIENT_COMMAND }

    data class Reply(val term: Long, val success: Boolean, val matchIndex: Long = 0)

    fun generarRequestId(): ByteArray {
        val uuid = UUID.randomUUID()
        return ByteBuffer.allocate(16).putLong(uuid.mostSignificantBits).putLong(uuid.leastSignificantBits).array()
    }

    fun leerHeader(body: ByteArray): Header {
        val input = DataInputStream(ByteArrayInputStream(body))
        require(input.readNBytes(4).contentEquals(MAGIC)) { "Firma DM2 inválida" }
        val algorithm = input.readUnsignedByte()
        val type = input.readUnsignedByte()
        val requestId = ByteArray(16).also(input::readFully)
        require(requestId.any { it != 0.toByte() }) { "ID de solicitud nulo" }
        return Header(algorithm, type, requestId, readId(input))
    }

    fun escribirHeader(out: DataOutputStream, type: Int, requestId: ByteArray, senderId: String,
                      algorithm: Int = ALGORITHM_RAFT) {
        out.write(MAGIC)
        out.writeByte(algorithm)
        out.writeByte(type)
        out.write(requestId)
        out.writeByte(senderId.length)
        out.write(senderId.toByteArray(Charsets.US_ASCII))
    }

    private fun readId(input: DataInputStream): String {
        val bytes = ByteArray(input.readUnsignedByte()).also(input::readFully)
        return String(bytes, Charsets.US_ASCII).also { require(raftIdPattern.matches(it)) }
    }

    private fun input(body: ByteArray, header: Header): DataInputStream {
        val offset = HEADER_BYTES + header.senderId.length
        return DataInputStream(ByteArrayInputStream(body, offset, body.size - offset))
    }

    fun validPosition(term: Long, index: Long, logTerm: Long): Boolean =
        term > 0 && index >= 0 && logTerm in 0..term && (index == 0L) == (logTerm == 0L)

    private fun readCommand(input: DataInputStream): ByteArray {
        val size = input.readInt()
        require(size in 1..65_536)
        return ByteArray(size).also(input::readFully)
    }

    fun requestUuid(bytes: ByteArray): UUID = ByteBuffer.wrap(bytes).let { UUID(it.long, it.long) }

    fun appendBudget(sender: String): Int = MAX_FRAME - HEADER_BYTES - sender.length - APPEND_FIXED_BYTES

    fun readRequest(body: ByteArray, header: Header): Request {
        val input = input(body, header)
        val request = when (header.type) {
            TYPE_RAFT_REQUEST_VOTE -> Vote(input.readLong(), input.readLong(), input.readLong()).also {
                require(validPosition(it.term, it.index, it.logTerm))
            }
            TYPE_RAFT_APPEND_ENTRIES -> {
                val term = input.readLong()
                val index = input.readLong()
                val logTerm = input.readLong()
                val commit = input.readLong()
                val count = input.readUnsignedShort()
                require(validPosition(term, index, logTerm) && commit >= 0)
                require(count <= MAX_APPEND_ENTRIES && index <= Long.MAX_VALUE - count)
                var lastTerm = logTerm
                val entries = List(count) { offset ->
                    val entryIndex = index + offset + 1
                    val entryTerm = input.readLong()
                    require(entryTerm > 0 && entryTerm in lastTerm..term)
                    lastTerm = entryTerm
                    when (input.readUnsignedByte()) {
                        0 -> EntradaLog(entryIndex, entryTerm)
                        1 -> {
                            val client = readId(input)
                            val token = requestUuid(ByteArray(16).also(input::readFully))
                            EntradaLog(entryIndex, entryTerm, client, token, readCommand(input))
                        }
                        else -> throw IllegalArgumentException("Tipo de entrada inválido")
                    }
                }
                Append(term, index, logTerm, commit, entries)
            }
            TYPE_CLIENT_COMMAND -> ClientCommand(readCommand(input))
            else -> error("Tipo DM2 inesperado")
        }
        require(input.available() == 0)
        return request
    }

    fun writeRequest(out: DataOutputStream, sender: String, requestId: ByteArray, request: Request) {
        writeFrame(out, sender, requestId, request.type) { payload ->
            when (request) {
                is Vote -> {
                    payload.writeLong(request.term)
                    payload.writeLong(request.index)
                    payload.writeLong(request.logTerm)
                }
                is Append -> {
                    payload.writeLong(request.term)
                    payload.writeLong(request.index)
                    payload.writeLong(request.logTerm)
                    payload.writeLong(request.commit)
                    payload.writeShort(request.entries.size)
                    request.entries.forEach { it.writeDm2(payload) }
                }
                is ClientCommand -> {
                    payload.writeInt(request.bytes.size)
                    payload.write(request.bytes)
                }
            }
        }
    }

    fun readReply(body: ByteArray, peer: String, requestId: ByteArray, request: Request): Reply? {
        val header = leerHeader(body)
        require(header.algorithm == ALGORITHM_RAFT && header.senderId == peer &&
            header.requestId.contentEquals(requestId))
        val input = input(body, header)
        if (header.type == TYPE_ERROR) {
            require(input.readUnsignedByte() in 1..4 && input.available() == 0)
            return null
        }
        require(header.type == (request.type or 0x80))
        val term = input.readLong()
        val flag = input.readUnsignedByte()
        require(flag in 0..1)
        val success = flag == 1
        val index = if (request is Append) input.readLong() else 0
        when (request) {
            is Vote -> require(term >= request.term)
            is Append -> require(term >= request.term && index == if (success) request.lastIndex else 0L)
            is ClientCommand -> error("Respuesta inesperada")
        }
        require(input.available() == 0)
        return Reply(term, success, index)
    }

    fun writeFrame(out: DataOutputStream, sender: String, requestId: ByteArray, type: Int,
                   algorithm: Int = ALGORITHM_RAFT, payload: (DataOutputStream) -> Unit) {
        val buffer = ByteArrayOutputStream()
        val body = DataOutputStream(buffer)
        escribirHeader(body, type, requestId, sender, algorithm)
        payload(body)
        require(buffer.size() in 1..MAX_FRAME) { "Longitud de trama inválida" }
        out.writeInt(buffer.size())
        buffer.writeTo(out)
        out.flush()
    }

    fun readFrame(socket: Socket, timeoutMs: Long): ByteArray {
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs)
        val input = socket.getInputStream()
        fun read(size: Int): ByteArray {
            val bytes = ByteArray(size)
            var offset = 0
            while (offset < size) {
                val remaining = deadline - System.nanoTime()
                if (remaining <= 0) throw SocketTimeoutException("Plazo de lectura agotado")
                socket.soTimeout = (TimeUnit.NANOSECONDS.toMillis(remaining) + 1).toInt()
                val count = input.read(bytes, offset, size - offset)
                if (count < 0) throw EOFException("Trama incompleta")
                offset += count
            }
            return bytes
        }
        val size = ByteBuffer.wrap(read(4)).int
        if (size !in 1..MAX_FRAME) throw IOException("Longitud de trama inválida")
        return read(size)
    }
}
