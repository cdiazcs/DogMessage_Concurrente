import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.InetSocketAddress
import java.net.Socket
import java.util.UUID

object Dm2Protocol {
    // Firma del protocolo DM2: 'D', 'M', '2', 0x00
    val MAGIC = byteArrayOf(0x44, 0x4D, 0x32, 0x00)

    // Algoritmos
    const val ALGORITHM_RAFT = 1

    // Tipos de mensajes Raft
    const val TYPE_RAFT_REQUEST_VOTE = 0x01
    const val TYPE_RAFT_REQUEST_VOTE_RESPONSE = 0x81
    const val TYPE_RAFT_APPEND_ENTRIES = 0x02
    const val TYPE_RAFT_APPEND_ENTRIES_RESPONSE = 0x82

    // Genera un requestId de 16 bytes (UUID sin guiones convertido a bytes)
    fun generarRequestId(): ByteArray {
        val uuid = UUID.randomUUID()
        val buffer = java.nio.ByteBuffer.allocate(16)
        buffer.putLong(uuid.mostSignificantBits)
        buffer.putLong(uuid.leastSignificantBits)
        return buffer.array()
    }

    // Lee un encabezado DM2 desde un arreglo de bytes
    data class Header(
        val algorithm: Int,
        val type: Int,
        val requestId: ByteArray,
        val senderId: String
    )

    fun leerHeader(body: ByteArray): Header {
        val input = DataInputStream(ByteArrayInputStream(body))
        val magic = ByteArray(4)
        input.readFully(magic)
        // Validar magic
        if (!magic.contentEquals(MAGIC)) throw IllegalArgumentException("Firma DM2 inválida")

        val algorithm = input.readUnsignedByte()
        val type = input.readUnsignedByte()

        val requestId = ByteArray(16)
        input.readFully(requestId)

        val senderLen = input.readUnsignedByte()
        val senderBytes = ByteArray(senderLen)
        input.readFully(senderBytes)
        val senderId = String(senderBytes, Charsets.US_ASCII)

        return Header(algorithm, type, requestId, senderId)
    }

    // Escribe el encabezado DM2 en un OutputStream
    fun escribirHeader(out: DataOutputStream, type: Int, requestId: ByteArray, senderId: String) {
        out.write(MAGIC)
        out.writeByte(ALGORITHM_RAFT)
        out.writeByte(type)
        out.write(requestId)
        
        val senderBytes = senderId.toByteArray(Charsets.US_ASCII)
        out.writeByte(senderBytes.size)
        out.write(senderBytes)
    }

    // --- MÉTODOS DE ENVÍO (CLIENTE) ---

    // Envía un RequestVote a otro nodo
    fun enviarRequestVote(targetHost: String, targetPort: Int, term: Long, lastLogIndex: Long, lastLogTerm: Long, myId: String): Boolean {
        try {
            Socket().use { socket ->
                socket.connect(InetSocketAddress(targetHost, targetPort), 2000)
                socket.soTimeout = 2000
                val out = DataOutputStream(socket.getOutputStream())

                val requestId = generarRequestId()
                // Payload: term (8 bytes) + lastLogIndex (8 bytes) + lastLogTerm (8 bytes)
                val payloadSize = 8 + 8 + 8
                val senderBytes = myId.toByteArray(Charsets.US_ASCII)
                // Longitud total = magic(4) + algo(1) + type(1) + reqId(16) + senderLen(1) + senderBytes + payload
                val bodyLength = 4 + 1 + 1 + 16 + 1 + senderBytes.size + payloadSize

                out.writeInt(bodyLength)
                escribirHeader(out, TYPE_RAFT_REQUEST_VOTE, requestId, myId)
                
                out.writeLong(term)
                out.writeLong(lastLogIndex)
                out.writeLong(lastLogTerm)
                out.flush()

                // Leer respuesta (RequestVoteResponse)
                val input = DataInputStream(socket.getInputStream())
                val resLength = input.readInt()
                val resBody = ByteArray(resLength)
                input.readFully(resBody)
                
                val resHeader = leerHeader(resBody)
                if (resHeader.type == TYPE_RAFT_REQUEST_VOTE_RESPONSE) {
                    val resInput = DataInputStream(ByteArrayInputStream(resBody))
                    resInput.skipBytes(4 + 1 + 1 + 16 + 1 + resHeader.senderId.length) // Saltar header
                    
                    val resTerm = resInput.readLong()
                    val voteGranted = resInput.readUnsignedByte() == 1
                    
                    // TODO: Aquí deberías verificar que resTerm >= term y actualizar tu estado si es necesario
                    return voteGranted
                }
            }
        } catch (e: Exception) {
            println("[RPC ERROR] Fallo RequestVote hacia $targetHost:$targetPort: ${e.message}")
        }
        return false
    }

    // Envía un Heartbeat / AppendEntries (simplificado por ahora para mantener vivo el liderazgo)
    fun enviarAppendEntries(targetHost: String, targetPort: Int, term: Long, prevLogIndex: Long, prevLogTerm: Long, leaderCommit: Long, myId: String, entryCount: Int = 0) {
        try {
            Socket().use { socket ->
                socket.connect(InetSocketAddress(targetHost, targetPort), 2000)
                val out = DataOutputStream(socket.getOutputStream())
                val requestId = generarRequestId()
                
                // Payload básico sin entradas (Heartbeat)
                val payloadSize = 8 + 8 + 8 + 8 + 2 // term, prevIndex, prevTerm, leaderCommit, entryCount
                val senderBytes = myId.toByteArray(Charsets.US_ASCII)
                val bodyLength = 4 + 1 + 1 + 16 + 1 + senderBytes.size + payloadSize
                
                out.writeInt(bodyLength)
                escribirHeader(out, TYPE_RAFT_APPEND_ENTRIES, requestId, myId)
                
                out.writeLong(term)
                out.writeLong(prevLogIndex)
                out.writeLong(prevLogTerm)
                out.writeLong(leaderCommit)
                out.writeShort(entryCount)
                
                out.flush()
                // En un Raft completo, aquí leerías la AppendEntriesResponse
            }
        } catch (e: Exception) {
            // Silencioso en heartbeats para no llenar la consola si un nodo está caído
        }
    }
}