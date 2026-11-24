import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import kotlin.concurrent.thread
import kotlin.random.Random

enum class EstadoRaft { FOLLOWER, CANDIDATE, LEADER }

data class EntradaLog(val index: Long, val term: Long, val comando: String = "")

class RaftNode(val myId: String, val puerto: Int, val otrosNodos: List<Pair<String, Int>>) {
    
    // --- VARIABLES DE ESTADO (Protegidas por lock) ---
    private val lock = Any()
    
    @Volatile private var estado = EstadoRaft.FOLLOWER
    @Volatile private var currentTerm = 0L
    @Volatile private var votedFor: String? = null
    
    // Log (en memoria por ahora, TODO: persistir en disco)
    private val log = mutableListOf<EntradaLog>()
    @Volatile private var commitIndex = 0L
    
    // Temporizadores
    @Volatile private var lastHeartbeatTime = System.currentTimeMillis()
    @Volatile private var electionTimeout = Random.nextLong(1500, 3000) // 1.5s a 3s

    fun iniciar() {
        println("[$myId] Nodo Raft iniciado como FOLLOWER. Término: $currentTerm")
        
        // 1. Hilo del Temporizador de Elecciones
        thread {
            while (true) {
                Thread.sleep(100)
                val tiempoActual = System.currentTimeMillis()
                
                synchronized(lock) {
                    if (estado != EstadoRaft.LEADER && (tiempoActual - lastHeartbeatTime) > electionTimeout) {
                        iniciarEleccion()
                    }
                }
            }
        }

        // 2. Hilo para Heartbeats (si soy líder)
        thread {
            while (true) {
                Thread.sleep(500) // Enviar heartbeat cada 500ms
                if (estado == EstadoRaft.LEADER) {
                    enviarHeartbeats()
                }
            }
        }
    }

    fun procesarMensajeDm2(body: ByteArray, output: DataOutputStream) {
        try {
            val header = Dm2Protocol.leerHeader(body)
            if (header.algorithm != Dm2Protocol.ALGORITHM_RAFT) return

            val input = DataInputStream(ByteArrayInputStream(body))
            input.skipBytes(4 + 1 + 1 + 16 + 1 + header.senderId.length)

            when (header.type) {
                Dm2Protocol.TYPE_RAFT_REQUEST_VOTE -> {
                    val term = input.readLong()
                    val lastLogIndex = input.readLong()
                    val lastLogTerm = input.readLong()
                    manejarRequestVote(header.senderId, header.requestId, term, lastLogIndex, lastLogTerm, output)
                }
                Dm2Protocol.TYPE_RAFT_APPEND_ENTRIES -> {
                    val term = input.readLong()
                    val prevLogIndex = input.readLong()
                    val prevLogTerm = input.readLong()
                    val leaderCommit = input.readLong()
                    manejarAppendEntries(header.senderId, term, prevLogIndex, prevLogTerm, leaderCommit)
                }
                
                // =========================================================
                // RECIBIR MENSAJE DE LA APP ANDROID Y DESENCRIPTAR
                // =========================================================
                0x03 -> {
                    val commandLen = input.readInt()
                    val commandBytes = ByteArray(commandLen)
                    input.readFully(commandBytes)
                    val mensajeEncriptado = String(commandBytes, Charsets.UTF_8)
                    
                    // Desencriptamos con la misma clave que pusimos en el QR
                    val mensajeDesencriptado = desencriptarAES(mensajeEncriptado, "CLAVE_SECRETA_DEL_QR")
                    
                    println("[$myId] ¡Mensaje recibido de la App Android!: $mensajeDesencriptado")
                }
            }
        } catch (e: Exception) {
            println("[$myId] Error procesando mensaje DM2: ${e.message}")
        }
    }

    private fun iniciarEleccion() {
        estado = EstadoRaft.CANDIDATE
        currentTerm += 1
        votedFor = myId
        lastHeartbeatTime = System.currentTimeMillis()
        electionTimeout = Random.nextLong(1500, 3000)
        
        println("[$myId] Iniciando elección. Término: $currentTerm")
        
        var votosRecibidos = 1
        val mayoria = (otrosNodos.size + 1) / 2 + 1
        
        val lastLogIndex = if (log.isEmpty()) 0L else log.last().index
        val lastLogTerm = if (log.isEmpty()) 0L else log.last().term

        for ((ip, puerto) in otrosNodos) {
            thread {
                val votoConcedido = Dm2Protocol.enviarRequestVote(ip, puerto, currentTerm, lastLogIndex, lastLogTerm, myId)
                if (votoConcedido) {
                    synchronized(lock) {
                        if (estado == EstadoRaft.CANDIDATE) {
                            votosRecibidos++
                            if (votosRecibidos >= mayoria) {
                                estado = EstadoRaft.LEADER
                                println("[$myId] ¡ELEGIDO LÍDER para el término $currentTerm!")
                                enviarHeartbeats()
                            }
                        }
                    }
                }
            }
        }
    }

    private fun manejarRequestVote(candidatoId: String, reqId: ByteArray, term: Long, lastLogIndex: Long, lastLogTerm: Long, output: DataOutputStream) {
        var votoConcedido = false
        
        synchronized(lock) {
            if (term > currentTerm) {
                currentTerm = term
                estado = EstadoRaft.FOLLOWER
                votedFor = null
            }

            if (term == currentTerm && (votedFor == null || votedFor == candidatoId)) {
                val miLastLogIndex = if (log.isEmpty()) 0L else log.last().index
                val miLastLogTerm = if (log.isEmpty()) 0L else log.last().term
                
                val logAlDia = (lastLogTerm > miLastLogTerm) || (lastLogTerm == miLastLogTerm && lastLogIndex >= miLastLogIndex)
                
                if (logAlDia) {
                    votoConcedido = true
                    votedFor = candidatoId
                    lastHeartbeatTime = System.currentTimeMillis()
                    electionTimeout = Random.nextLong(1500, 3000)
                }
            }
        }

        println("[$myId] RequestVote de $candidatoId (Término $term). Concedido: $votoConcedido")

        try {
            val payloadSize = 8 + 1
            val senderBytes = myId.toByteArray(Charsets.US_ASCII)
            val bodyLength = 4 + 1 + 1 + 16 + 1 + senderBytes.size + payloadSize
            
            output.writeInt(bodyLength)
            Dm2Protocol.escribirHeader(output, Dm2Protocol.TYPE_RAFT_REQUEST_VOTE_RESPONSE, reqId, myId)
            
            output.writeLong(currentTerm)
            output.writeByte(if (votoConcedido) 1 else 0)
            output.flush()
        } catch (e: Exception) {
            println("Error enviando RequestVoteResponse: ${e.message}")
        }
    }

    private fun manejarAppendEntries(liderId: String, term: Long, prevLogIndex: Long, prevLogTerm: Long, leaderCommit: Long) {
        synchronized(lock) {
            if (term < currentTerm) return
            
            estado = EstadoRaft.FOLLOWER
            currentTerm = term
            lastHeartbeatTime = System.currentTimeMillis()
            electionTimeout = Random.nextLong(1500, 3000)
        }
    }

    private fun enviarHeartbeats() {
        val lastLogIndex = if (log.isEmpty()) 0L else log.last().index
        val lastLogTerm = if (log.isEmpty()) 0L else log.last().term
        
        for ((ip, puerto) in otrosNodos) {
            thread {
                Dm2Protocol.enviarAppendEntries(ip, puerto, currentTerm, lastLogIndex, lastLogTerm, commitIndex, myId)
            }
        }
    }

    // =========================================================
    // FUNCIÓN PARA DESENCRIPTAR EL MENSAJE (AES)
    // =========================================================
    private fun desencriptarAES(textoEncriptadoBase64: String, claveSecreta: String): String {
        return try {
            val claveBytes = ByteArray(16)
            val clavesOriginales = claveSecreta.toByteArray(Charsets.UTF_8)
            System.arraycopy(clavesOriginales, 0, claveBytes, 0, minOf(clavesOriginales.size, 16))
            
            val secretKey = javax.crypto.spec.SecretKeySpec(claveBytes, "AES")
            val iv = byteArrayOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
            val cipher = javax.crypto.Cipher.getInstance("AES/CBC/PKCS5Padding")
            cipher.init(javax.crypto.Cipher.DECRYPT_MODE, secretKey, javax.crypto.spec.IvParameterSpec(iv))
            
            val bytesDecodificados = java.util.Base64.getDecoder().decode(textoEncriptadoBase64)
            val bytesDesencriptados = cipher.doFinal(bytesDecodificados)
            String(bytesDesencriptados, Charsets.UTF_8)
        } catch (e: Exception) {
            "[Error de desencriptación: ${e.message}]"
        }
    }
}