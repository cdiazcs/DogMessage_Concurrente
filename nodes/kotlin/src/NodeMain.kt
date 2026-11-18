import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.Executors
import kotlin.system.exitProcess

// Nodo TCP interoperable de conectividad. Raft NO está implementado.
private const val MAX_FRAME = 1024 * 1024
private val idRegex = Regex("[a-zA-Z0-9_-]{1,32}")
private val reqRegex = Regex("[a-fA-F0-9]{32}")

private fun sendFrame(out: DataOutputStream, body: String) {
    val data = body.toByteArray(StandardCharsets.UTF_8)
    require(data.isNotEmpty() && data.size <= MAX_FRAME) { "Longitud de trama inválida" }
    out.writeInt(data.size) // Big endian
    out.write(data)
    out.flush()
}

private fun readFrame(input: DataInputStream): String {
    val length = input.readInt()
    require(length > 0 && length <= MAX_FRAME) { "Longitud de trama inválida" }
    val data = ByteArray(length)
    input.readFully(data)
    return String(data, StandardCharsets.UTF_8)
}

private fun handle(socket: Socket, nodeId: String) {
    try {
        socket.use { client ->
            client.soTimeout = 5000
            val input = DataInputStream(client.getInputStream())
            val output = DataOutputStream(client.getOutputStream())
            val parts = readFrame(input).split('|')
            if (parts.size != 4 || parts[0] != "DM1" || parts[1] != "PING"
                || !idRegex.matches(parts[2]) || !reqRegex.matches(parts[3])) {
                sendFrame(output, "DM1|ERROR|$nodeId|00000000000000000000000000000000")
            } else {
                sendFrame(output, "DM1|PONG|$nodeId|${parts[3]}")
            }
        }
    } catch (_: Exception) {
        // Una desconexión no debe terminar el servidor.
    }
}

private fun serve(nodeId: String, host: String, port: Int) {
    ServerSocket().use { server ->
        server.reuseAddress = true
        server.bind(InetSocketAddress(host, port))
        val pool = Executors.newFixedThreadPool(16)
        println("[KOTLIN $nodeId] LISTEN $host:$port protocol=DM1")
        while (true) {
            val client = server.accept()
            pool.execute { handle(client, nodeId) }
        }
    }
}

private fun ping(nodeId: String, endpoint: String): Int {
    val index = endpoint.lastIndexOf(':')
    require(index > 0) { "Usa --ping IP:PUERTO" }
    val host = endpoint.substring(0, index)
    val port = endpoint.substring(index + 1).toInt()
    val requestId = UUID.randomUUID().toString().replace("-", "")
    Socket().use { socket ->
        socket.connect(InetSocketAddress(host, port), 5000)
        socket.soTimeout = 5000
        sendFrame(DataOutputStream(socket.getOutputStream()), "DM1|PING|$nodeId|$requestId")
        val response = readFrame(DataInputStream(socket.getInputStream())).split('|')
        if (response.size == 4 && response[0] == "DM1" && response[1] == "PONG"
            && idRegex.matches(response[2]) && response[3] == requestId) {
            println("OK $nodeId -> $endpoint: PONG de ${response[2]}")
            return 0
        }
        System.err.println("ERROR respuesta inesperada")
        return 1
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
        if (target == null) serve(id, host, port) else exitProcess(ping(id, target))
    } catch (e: Exception) {
        System.err.println("ERROR: ${e.message}")
        exitProcess(1)
    }
}
