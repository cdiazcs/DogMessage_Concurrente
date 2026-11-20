import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.Executors
import kotlin.system.exitProcess

// --- LÓGICA DM1 (Se mantiene para diagnóstico y compatibilidad) ---
private const val MAX_FRAME = 1024 * 1024
private val idRegex = Regex("[a-zA-Z0-9_-]{1,32}")
private val reqRegex = Regex("[a-fA-F0-9]{32}")

private fun sendDm1Frame(out: DataOutputStream, body: String) {
    val data = body.toByteArray(StandardCharsets.UTF_8)
    require(data.isNotEmpty() && data.size <= MAX_FRAME) { "Longitud de trama inválida" }
    out.writeInt(data.size) // Big endian
    out.write(data)
    out.flush()
}

private fun handleDm1(body: ByteArray, output: DataOutputStream, nodeId: String) {
    val text = String(body, StandardCharsets.UTF_8)
    val parts = text.split('|')
    if (parts.size != 4 || parts[0] != "DM1" || parts[1] != "PING"
        || !idRegex.matches(parts[2]) || !reqRegex.matches(parts[3])) {
        sendDm1Frame(output, "DM1|ERROR|$nodeId|00000000000000000000000000000000")
    } else {
        sendDm1Frame(output, "DM1|PONG|$nodeId|${parts[3]}")
    }
}

private fun pingDm1(nodeId: String, endpoint: String): Int {
    val index = endpoint.lastIndexOf(':')
    require(index > 0) { "Usa --ping IP:PUERTO" }
    val host = endpoint.substring(0, index)
    val port = endpoint.substring(index + 1).toInt()
    val requestId = UUID.randomUUID().toString().replace("-", "")
    Socket().use { socket ->
        socket.connect(InetSocketAddress(host, port), 5000)
        socket.soTimeout = 5000
        sendDm1Frame(DataOutputStream(socket.getOutputStream()), "DM1|PING|$nodeId|$requestId")
        
        // Leer respuesta DM1
        val input = DataInputStream(socket.getInputStream())
        val length = input.readInt()
        val data = ByteArray(length)
        input.readFully(data)
        val response = String(data, StandardCharsets.UTF_8).split('|')
        
        if (response.size == 4 && response[0] == "DM1" && response[1] == "PONG"
            && idRegex.matches(response[2]) && response[3] == requestId) {
            println("OK $nodeId -> $endpoint: PONG de ${response[2]}")
            return 0
        }
        System.err.println("ERROR respuesta inesperada")
        return 1
    }
}
// -------------------------------------------------------------------

// --- LÓGICA PRINCIPAL (Servidor Mixto DM1 / DM2) ---
private fun handle(socket: Socket, nodeId: String, raftNode: RaftNode?) {
    try {
        socket.use { client ->
            client.soTimeout = 5000
            val input = DataInputStream(client.getInputStream())
            val output = DataOutputStream(client.getOutputStream())
            
            // 1. Leer prefijo (4 bytes de longitud)
            val bodyLength = input.readInt()
            if (bodyLength < 1 || bodyLength > MAX_FRAME) return // Inválido
            
            // 2. Leer el cuerpo completo
            val body = ByteArray(bodyLength)
            input.readFully(body)
            
            // 3. ¿Es DM1 o DM2?
            if (bodyLength >= 4 && body[0] == 'D'.code.toByte() && body[1] == 'M'.code.toByte() && body[2] == '1'.code.toByte()) {
                // Es DM1 (PING/PONG)
                handleDm1(body, output, nodeId)
            } else if (bodyLength >= 4 && body[0] == 0x44.toByte() && body[1] == 0x4D.toByte() && body[2] == 0x32.toByte() && body[3] == 0x00.toByte()) {
                // Es DM2 (Binario Raft)
                if (raftNode != null) {
                    raftNode.procesarMensajeDm2(body, output)
                } else {
                    println("Error: Nodo Raft no inicializado")
                }
            } else {
                println("Trama desconocida recibida, cerrando.")
            }
        }
    } catch (_: Exception) {
        // Ignorar desconexiones
    }
}

private fun serve(nodeId: String, host: String, port: Int, raftNode: RaftNode?) {
    ServerSocket().use { server ->
        server.reuseAddress = true
        server.bind(InetSocketAddress(host, port))
        val pool = Executors.newFixedThreadPool(16)
        println("[KOTLIN $nodeId] LISTEN $host:$port protocol=DM1+DM2")
        while (true) {
            val client = server.accept()
            pool.execute { handle(client, nodeId, raftNode) }
        }
    }
}

fun main(args: Array<String>) {
    try {
        var id: String? = null
        var host = "0.0.0.0"
        var port = 9103
        var target: String? = null
        var i = 0
        while (i < args.size) {
            require(i + 1 < args.size) { "Falta el valor de ${args[i]}" }
            val value = args[i + 1]
            when (args[i]) {
                "--id" -> id = value
                "--host" -> host = value
                "--port" -> port = value.toInt()
                "--ping" -> target = value
                else -> error("Argumento desconocido ${args[i]}")
            }
            i += 2
        }
        require(id != null && idRegex.matches(id)) { "--id obligatorio (1-32: letras, números, _ o -)" }
        
        if (target != null) {
            // Modo Cliente: Solo hace Ping por DM1 y termina
            exitProcess(pingDm1(id, target))
        } else {
            // Configuración para prueba local con 3 nodos en la misma PC
            val todosLosNodos = listOf(
                "127.0.0.1" to 9103,
                "127.0.0.1" to 9104,
                "127.0.0.1" to 9105
            )
            
            // Filtramos para no incluir nuestro propio puerto y evitar auto-votar
            val otrosNodos = todosLosNodos.filter { it.second != port }
            
            // Inicializamos el nodo Raft
            val raftNode = RaftNode(id, port, otrosNodos)
            raftNode.iniciar() // Inicia el hilo de elecciones
            
            // Iniciamos el servidor TCP
            serve(id, host, port, raftNode)
        }
    } catch (e: Exception) {
        System.err.println("ERROR: ${e.message}")
        exitProcess(1)
    }
}