import dogmessenger.twopc.Engine
import dogmessenger.chat.Feed
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.net.ServerSocket
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
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
        sendDm1Frame(DataOutputStream(socket.getOutputStream()), "DM1|PING|$nodeId|$requestId")
        
        val data = Dm2Protocol.readFrame(socket, 5_000)
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
private fun handle(socket: Socket, nodeId: String, dm2Handler: ((ByteArray, DataOutputStream) -> Unit)?) {
    socket.use { client ->
        try {
            val body = Dm2Protocol.readFrame(client, 5_000)
            val output = DataOutputStream(client.getOutputStream())
            if (body.size >= 4) {
                when (body.copyOfRange(0, 4).decodeToString()) {
                    "DM1|" -> handleDm1(body, output, nodeId)
                    "DM2\u0000" -> dm2Handler?.invoke(body, output)
                }
            }
        } catch (_: IOException) {
            // Desconexión, trama incompleta o fallo de persistencia ya registrado por el motor.
        } catch (_: IllegalArgumentException) {
            // Encabezado inválido.
        }
    }
}

private fun serve(nodeId: String, host: String, port: Int, dm2Handler: ((ByteArray, DataOutputStream) -> Unit)?,
                  onStart: () -> Unit = {}, onStop: () -> Unit = {}) {
    ServerSocket().use { server ->
        server.reuseAddress = true
        server.bind(InetSocketAddress(host, port))
        val pool = ThreadPoolExecutor(16, 16, 0, TimeUnit.MILLISECONDS, SynchronousQueue())
        val clients = ConcurrentHashMap.newKeySet<Socket>()
        fun stop() {
            server.close()
            pool.shutdownNow()
            clients.forEach { runCatching { it.close() } }
            onStop()
        }
        val hook = Thread(::stop, "kotlin-raft-shutdown")
        val runtime = Runtime.getRuntime()
        runtime.addShutdownHook(hook)
        try {
            onStart()
            println("[KOTLIN $nodeId] LISTEN $host:$port protocol=${if (dm2Handler == null) "DM1" else "DM1+DM2"}")
            while (!server.isClosed) {
                val client = try {
                    server.accept()
                } catch (error: IOException) {
                    if (server.isClosed) break
                    throw error
                }
                clients.add(client)
                try {
                    pool.execute {
                        try {
                            handle(client, nodeId, dm2Handler)
                        } finally {
                            clients.remove(client)
                        }
                    }
                } catch (_: RejectedExecutionException) {
                    clients.remove(client)
                    client.close()
                }
            }
        } finally {
            stop()
            try {
                runtime.removeShutdownHook(hook)
            } catch (_: IllegalStateException) {
                // El hook ya está ejecutándose.
            }
        }
    }
}

private fun parsePeer(value: String): Pair<String, InetSocketAddress> {
    val (id, endpoint) = value.split('=', limit = 2).also {
        require(it.size == 2 && raftIdPattern.matches(it[0])) { "Usa --peer ID=HOST:PUERTO" }
    }
    val separator = endpoint.lastIndexOf(':')
    require(separator > 0) { "Usa --peer ID=HOST:PUERTO" }
    val host = endpoint.substring(0, separator)
    val port = endpoint.substring(separator + 1).toInt()
    require(port in 1..65535) { "Puerto de peer inválido" }
    val address = InetAddress.getByName(host)
    require(!address.isAnyLocalAddress && !address.isMulticastAddress) { "Dirección de peer inválida" }
    return id to InetSocketAddress(address, port)
}

fun main(args: Array<String>) {
    try {
        var id: String? = null
        var host = "0.0.0.0"
        var port = 9103
        var target: String? = null
        var dataDir: Path? = null
        val peers = linkedMapOf<String, InetSocketAddress>()
        var algorithm: String? = null
        var coordinator: String? = null
        var chatFeed = false
        var configuredMembers: List<String>? = null
        val modes = mutableSetOf<String>()
        var i = 0
        while (i < args.size) {
            val option = args[i++]
            if (option == "--chat-feed") {
                require(!chatFeed) { "--chat-feed repetido" }
                chatFeed = true
                continue
            }
            if (option in setOf("--diagnostic", "--inspect", "--local-raft", "--experimental-raft")) {
                require(modes.add(option)) { "Opción repetida: $option" }
                continue
            }
            require(i < args.size && !args[i].startsWith("--")) { "Falta el valor de $option" }
            val value = args[i++]
            when (option) {
                "--id" -> id = value
                "--host" -> host = value
                "--port" -> port = value.toInt()
                "--ping" -> target = value
                "--data-dir" -> {
                    require(value.isNotBlank()) { "--data-dir requiere una ruta" }
                    dataDir = Path.of(value)
                }
                "--algo" -> {
                    require(algorithm == null && value in setOf("raft", "2pc")) { "Usa --algo raft o 2pc una sola vez" }
                    algorithm = value
                }
                "--coordinator" -> coordinator = value
                "--members" -> {
                    require(configuredMembers == null) { "--members repetido" }
                    configuredMembers = value.split(',')
                }
                "--peer" -> {
                    val (peer, endpoint) = parsePeer(value)
                    require(peers.putIfAbsent(peer, endpoint) == null) { "Peer repetido: $peer" }
                }
                else -> error("Argumento desconocido $option")
            }
        }
        require(id != null && idRegex.matches(id)) { "--id obligatorio (1-32: letras, números, _ o -)" }
        require(host.isNotBlank() && port in 1..65535) { "Host o puerto local inválido" }
        require(modes.size + (if (target == null) 0 else 1) <= 1) { "Selecciona un solo modo" }
        val mode = modes.singleOrNull()
        val fullRaft = algorithm == "raft"
        val twoPc = algorithm == "2pc"
        require(!chatFeed || (host == "127.0.0.1" && dataDir != null && target == null && mode == null &&
            (fullRaft || twoPc))) { "--chat-feed requiere servidor persistente en 127.0.0.1" }
        val chatKey = if (chatFeed) Feed.key() else null
        require(coordinator == null || twoPc) { "--coordinator requiere 2PC" }
        require(!twoPc || (target == null && dataDir != null && coordinator != null &&
            (mode == null || mode == "--inspect"))) { "2PC requiere --data-dir y --coordinator" }
        require(!fullRaft || (target == null && (mode == null || mode == "--inspect"))) {
            "--algo raft es incompatible con el modo seleccionado"
        }
        require(!fullRaft || dataDir != null) { "--algo raft requiere --data-dir" }
        val experimental = mode == "--experimental-raft"
        val passive = mode == "--local-raft"
        require(peers.isEmpty() || experimental || passive || fullRaft || twoPc || mode == "--inspect") {
            "--peer requiere --algo raft, --experimental-raft, --local-raft o --inspect"
        }
        require(id !in peers && peers.values.toSet().size == peers.size) { "Identidad o dirección de peer duplicada" }
        val members = configuredMembers
        if (members != null) {
            require(experimental || passive || fullRaft || twoPc || mode == "--inspect") { "--members requiere un modo Raft" }
            require(members.size == members.toSet().size && members.toSet() == peers.keys + id) {
                "--members debe coincidir con el ID local y los peers, sin duplicados"
            }
        }
        if (experimental || passive || ((fullRaft || twoPc) && mode != "--inspect")) {
            require(dataDir != null && peers.isNotEmpty()) { "El modo Raft requiere --data-dir y --peer" }
            val local = InetAddress.getByName(host)
            require(peers.values.none {
                it.port == port && (it.address == local ||
                    (local.isAnyLocalAddress && NetworkInterface.getByInetAddress(it.address) != null))
            }) { "Un peer usa el endpoint local" }
        }
        if (passive) {
            val address = InetAddress.getByName(host)
            require(address.isLoopbackAddress && peers.values.all { it.address.isLoopbackAddress }) {
                "--local-raft requiere host y peers de loopback"
            }
            host = address.hostAddress
        }

        when {
            target != null -> {
                require(dataDir == null) { "--ping no usa --data-dir" }
                exitProcess(pingDm1(id, target))
            }
            twoPc -> Engine(id, checkNotNull(coordinator), peers, checkNotNull(dataDir), mode == "--inspect").use { engine ->
                if (mode == "--inspect") engine.inspect()
                else serve(id, host, port, { body, output ->
                    val response = if (chatKey != null && Feed.isFeed(body))
                        Feed.dispatch(body, id, chatKey, engine::chatSnapshot) else engine.dispatch(body)
                    if (response != null) {
                        output.writeInt(response.size)
                        output.write(response)
                        output.flush()
                    }
                }, onStart = engine::start, onStop = engine::close)
            }
            dataDir != null -> {
                require(!Files.exists(dataDir.resolve("twopc.lock"))) { "Raft exige un directorio distinto de 2PC" }
                RaftStorage(dataDir, id).use { storage ->
                    PersistentRaftNode(id, peers, storage, replication = fullRaft).use { node ->
                        if (mode == "--inspect") println(node.inspect())
                        else {
                            val handler: ((ByteArray, DataOutputStream) -> Unit)? = when {
                                chatKey != null -> { body, output ->
                                    if (Feed.isFeed(body)) {
                                        val reply = Feed.dispatch(body, id, chatKey, node::chatSnapshot)
                                        if (reply != null) {
                                            output.writeInt(reply.size)
                                            output.write(reply)
                                            output.flush()
                                        }
                                    } else node.handleDm2(body, output)
                                }
                                fullRaft || experimental || passive -> node::handleDm2
                                else -> null
                            }
                            serve(id, host, port, handler, onStart = { if (fullRaft || experimental) node.start() },
                                onStop = { node.close(); storage.close() })
                        }
                    }
                }
            }
            else -> {
                require(mode != "--inspect") { "--inspect requiere --data-dir" }
                serve(id, host, port, null)
            }
        }
    } catch (e: Exception) {
        System.err.println("ERROR: ${e.message}")
        exitProcess(1)
    }
}
