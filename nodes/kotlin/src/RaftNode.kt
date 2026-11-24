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
                    // Si no soy líder y pasó el tiempo de timeout, inicio elección
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

    // Llamado desde NodeMain cuando entra un mensaje DM2
    fun procesarMensajeDm2(body: ByteArray, output: DataOutputStream) {
        try {
            val header = Dm2Protocol.leerHeader(body)
            if (header.algorithm != Dm2Protocol.ALGORITHM_RAFT) return

            // Creamos un stream para leer el payload (saltando el header)
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
                    // TODO: Leer entryCount y las entradas
                    manejarAppendEntries(header.senderId, term, prevLogIndex, prevLogTerm, leaderCommit)
                }
                
                // =========================================================
                // NUEVO CASO: Recibir mensaje desde la App Android (0x03)
                // =========================================================
                0x03 -> {
                    val commandLen = input.readInt()
                    val commandBytes = ByteArray(commandLen)
                    input.readFully(commandBytes)
                    val mensajeTexto = String(commandBytes, Charsets.UTF_8)
                    
                    println("[$myId] ¡Mensaje recibido de la App Android!: $mensajeTexto")
                    
                    // TODO: Si este nodo es el LÍDER, debería agregar este mensaje a su log
                    // y enviarlo a los demás nodos por AppendEntries para replicarlo.
                    // Por ahora, solo lo mostramos en consola para confirmar la conexión.
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
        
        // Voto por mí mismo
        var votosRecibidos = 1
        val mayoria = (otrosNodos.size + 1) / 2 + 1
        
        // Calcular último log
        val lastLogIndex = if (log.isEmpty()) 0L else log.last().index
        val lastLogTerm = if (log.isEmpty()) 0L else log.last().term

        for ((ip, puerto) in otrosNodos) {
            thread {
                val votoConcedido = Dm2Protocol.enviarRequestVote(ip, puerto, currentTerm, lastLogIndex, lastLogTerm, myId)
                if (votoConcedido) {
                    synchronized(lock) {
                        if (estado == EstadoRaft.CANDIDATE) { // Verificar que siga siendo candidato
                            votosRecibidos++
                            if (votosRecibidos >= mayoria) {
                                estado = EstadoRaft.LEADER
                                println("[$myId] ¡ELEGIDO LÍDER para el término $currentTerm!")
                                enviarHeartbeats() // Enviar inmediatamente al ser elegido
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
            // 1. Si el término del candidato es mayor, actualizo mi término y paso a seguidor
            if (term > currentTerm) {
                currentTerm = term
                estado = EstadoRaft.FOLLOWER
                votedFor = null
                // TODO: Persistir currentTerm y votedFor aquí
            }

            // 2. Evalúo si le doy el voto
            if (term == currentTerm && (votedFor == null || votedFor == candidatoId)) {
                // Verificar que su log sea al menos tan actual como el mío
                val miLastLogIndex = if (log.isEmpty()) 0L else log.last().index
                val miLastLogTerm = if (log.isEmpty()) 0L else log.last().term
                
                val logAlDia = (lastLogTerm > miLastLogTerm) || (lastLogTerm == miLastLogTerm && lastLogIndex >= miLastLogIndex)
                
                if (logAlDia) {
                    votoConcedido = true
                    votedFor = candidatoId
                    lastHeartbeatTime = System.currentTimeMillis() // Reiniciar timer porque le di el voto
                    electionTimeout = Random.nextLong(1500, 3000) // Reiniciar timeout
                    // TODO: Persistir votedFor aquí
                }
            }
        }

        println("[$myId] RequestVote de $candidatoId (Término $term). Concedido: $votoConcedido")

        // Enviar respuesta
        try {
            val payloadSize = 8 + 1 // term (R64) + voteGranted (Bool u8)
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
            if (term < currentTerm) {
                return // Ignorar
            }
            
            // Es un líder válido, reiniciar timeout
            estado = EstadoRaft.FOLLOWER
            currentTerm = term
            lastHeartbeatTime = System.currentTimeMillis()
            electionTimeout = Random.nextLong(1500, 3000) // <-- IMPORTANTE: Evita que el seguidor inicie elecciones
            
            // TODO: Verificar prevLogIndex/prevLogTerm contra nuestro log local.
            // Si coincide, aceptar entradas y actualizar commitIndex.
            // Si no coincide, responder con success=0.
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
}