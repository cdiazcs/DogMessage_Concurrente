package com.pc4.dogmessenger

import android.app.Activity
import android.app.AlertDialog
import android.content.Intent
import android.widget.ImageView
import android.graphics.Color
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.text.InputType
import android.view.View
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import org.json.JSONArray
import org.json.JSONObject
import java.util.concurrent.Executors

class MainActivity : Activity() {
    private val worker = Executors.newSingleThreadExecutor()
    private val mediaWorker = Executors.newSingleThreadExecutor()
    private var mediaBusy = false
    private var upload: AttachmentClient.Upload? = null
    private var selectedConversation: String? = null
    private data class Download(val item: JSONObject, val ticket: JSONObject?, val conversation: String)
    private var selectedDownload: Download? = null
    private val handler = Handler(Looper.getMainLooper())
    private lateinit var layout: LinearLayout
    private lateinit var connection: TextView
    private lateinit var feedback: TextView
    private lateinit var network: NetworkClient
    private var endpoint = ""
    private var active = false
    private var busy = false
    private var session: String? = null
    private var localIdentity: MobileE1.Device? = null
    private val secure by lazy { AndroidIdentity(applicationContext) }
    private var own = ""
    private var username = ""
    private var minimum = "0"
    private var current: JSONObject? = null
    private var cursor = "0"
    private var more = false
    private var directory = emptyList<JSONObject>()
    private val messages = linkedMapOf<String, JSONObject>()
    private var pending: GatewayRequest? = null
    private var credentialAttempt: GatewayRequest? = null
    private var draft = ""
    private var feedbackText = ""
    private var algorithm = ""
    private var tick = 0
    private val poll = object : Runnable {
        override fun run() {
            if (!active) return
            if (!busy && session != null) {
                if (current == null || ++tick % 6 == 0) refresh()
                else if (!more) page("sync")
            }
            handler.postDelayed(this, 3500)
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        loginView()
    }

    override fun onStart() {
        super.onStart()
        active = true
        handler.postDelayed(poll, 3500)
    }

    override fun onStop() {
        active = false
        handler.removeCallbacks(poll)
        super.onStop()
    }

    override fun onDestroy() {
        handler.removeCallbacksAndMessages(null)
        worker.shutdownNow()
        mediaWorker.shutdownNow()
        if (!mediaBusy) upload?.staged?.delete()
        super.onDestroy()
    }

    private fun screen(title: String) {
        layout = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(24, 24, 24, 24)
            setBackgroundColor(0xfff8fafc.toInt())
        }
        setContentView(ScrollView(this).apply { addView(layout) })
        addText("Dog Messenger", 26f).apply {
            setBackgroundColor(0xff164e63.toInt())
            setTextColor(Color.WHITE)
            setPadding(12, 10, 12, 10)
        }
        connection = addText(if (algorithm.isEmpty()) "Sin sesión" else "Conectado · " + algorithm.uppercase())
        addText(title, 21f)
        feedback = addText(feedbackText)
        feedback.accessibilityLiveRegion = View.ACCESSIBILITY_LIVE_REGION_POLITE
    }

    private fun addText(value: String, size: Float = 16f): TextView =
        TextView(this).apply {
            text = value
            textSize = size
            setTextColor(0xff173747.toInt())
            layout.addView(this)
        }

    private fun field(hintText: String, type: Int = InputType.TYPE_CLASS_TEXT, default: String = ""): EditText =
        EditText(this).apply {
            hint = hintText
            inputType = type
            setText(default)
            layout.addView(this)
        }

    private fun button(label: String, action: () -> Unit) {
        layout.addView(Button(this).apply {
            text = label
            setOnClickListener { if (!busy) action() }
        })
    }

    private fun info(message: String) {
        feedbackText = message
        feedback.text = message
    }

    private fun clearSession() {
        session = null
        localIdentity = null
        upload?.cancel?.set(true)
        if (!mediaBusy) upload?.staged?.delete()
        upload = null
        credentialAttempt = null
        pending = null
        minimum = "0"
        algorithm = ""
        directory = emptyList()
        current = null
        messages.clear()
        draft = ""
        loginView()
    }

    private fun loginView() {
        screen("Tu cuenta")
        val machine = field("Servidor TLS", default = endpoint.substringBefore(':').ifBlank { "10.0.2.2" })
        val port = field("Puerto", InputType.TYPE_CLASS_NUMBER, endpoint.substringAfter(':', "9443"))
        val user = field("Usuario", default = username)
        val name = field("Nombre para registro")
        val password = field("Contraseña", InputType.TYPE_CLASS_TEXT or InputType.TYPE_TEXT_VARIATION_PASSWORD)
        fun authenticate(op: String) {
            val host = machine.text.toString().trim()
            val number = port.text.toString().toIntOrNull()
            if (host.isBlank() || number == null || number !in 1..65535) {
                info("Dirección o puerto inválido")
                return
            }
            val address = "$host:$number"
            if (address != endpoint) credentialAttempt = null
            endpoint = address
            network = NetworkClient(host, number)
            username = user.text.toString().trim()
            val data = JSONObject().put("username", username).put("password", password.text.toString())
            if (op == "register") data.put("display", name.text.toString())
            val previous = credentialAttempt
            val attempt = if (previous != null && previous.operation == op && previous.data.toString() == data.toString()) previous
                else GatewayRequest(op, data)
            credentialAttempt = attempt
            request(attempt) { value ->
                val result = value as JSONObject
                if (showResult(result)) {
                    credentialAttempt = null
                    password.text.clear()
                }
                val token = result.optString("session")
                if (token.isNotEmpty()) {
                    session = token
                    refresh(true)
                }
            }
        }
        button("Iniciar sesión") { authenticate("login") }
        button("Registrar") { authenticate("register") }
    }

    private fun refresh(refreshUsers: Boolean = false) {
        if (refreshUsers) {
            request(GatewayRequest("users")) { value ->
                directory = rows(value as JSONArray)
                refresh()
            }
            return
        }
        request(GatewayRequest("conversations")) { value ->
            val result = value as JSONObject
            own = result.getString("user")
            val conversations = rows(result.getJSONArray("items"))
            val selected = current?.getString("id")
            val alternative = conversations.firstOrNull { it.getString("id") == selected }
            if (alternative == null) {
                current = null
                conversationsView(conversations)
            } else if (alternative.optBoolean("e2ee") != current?.optBoolean("e2ee")) {
                current = alternative
                thread()
            }
        }
    }

    private fun conversationsView(conversations: List<JSONObject>) {
        screen(username)
        button("Actualizar usuarios") { refresh(true) }
        button("Identidad E2EE") { registerDevice() }
        button("Vincular dispositivo") { linkMenu() }
        button("Dispositivos vinculados") { devices() }
        button("Conversación privada") { chooseUser { id -> mutate("direct", JSONObject().put("peer", id)) } }
        button("Crear grupo") {
            val name = EditText(this).apply { hint = "Nombre del grupo" }
            AlertDialog.Builder(this).setTitle("Nuevo grupo").setView(name)
                .setPositiveButton("Crear") { _, _ -> chooseGroup(name.text.toString()) }
                .setNegativeButton("Cancelar", null).show()
        }
        button("Cerrar sesión") {
            request(GatewayRequest("logout")) { value ->
                val result = value as JSONObject
                showResult(result)
                if (isApplied(result)) clearSession()
            }
        }
        addText("Conversaciones", 20f)
        for (item in conversations) {
            button(conversationLabel(item)) {
                current = item
                cursor = "0"
                more = false
                draft = ""
                messages.clear()
                thread()
                page("history")
            }
        }
    }

    private fun label(id: String) = directory.firstOrNull { it.getString("id") == id }?.getString("display") ?: id

    private fun conversationLabel(item: JSONObject): String {
        if (!item.isNull("name")) return item.getString("name")
        val members = item.getJSONArray("members")
        val peer = (0 until members.length()).map(members::getString).firstOrNull { it != own }
        return if (peer == null) "Conversación" else label(peer)
    }

    private fun chooseUser(select: (String) -> Unit) {
        val candidates = directory.filter { it.getString("id") != own }
        if (candidates.isEmpty()) { info("No hay usuarios disponibles"); return }
        AlertDialog.Builder(this).setTitle("Elige un usuario")
            .setItems(candidates.map { it.getString("display") }.toTypedArray()) { _, i ->
                if (!busy) select(candidates[i].getString("id"))
            }.show()
    }

    private fun chooseGroup(name: String) {
        val candidates = directory.filter { it.getString("id") != own }
        val checked = BooleanArray(candidates.size)
        AlertDialog.Builder(this).setTitle("Integrantes")
            .setMultiChoiceItems(candidates.map { it.getString("display") }.toTypedArray(), checked) { _, i, flag ->
                checked[i] = flag
            }
            .setPositiveButton("Crear") { _, _ ->
                val members = JSONArray()
                candidates.forEachIndexed { i, user -> if (checked[i]) members.put(user.getString("id")) }
                mutate("group-create", JSONObject().put("name", name).put("members", members))
            }.setNegativeButton("Cancelar", null).show()
    }

    private fun thread() {
        val conversation = current ?: return
        val id = conversation.getString("id")
        screen(conversationLabel(conversation))
        button("Volver") { current = null; refresh() }
        if (conversation.getString("type") == "GROUP") {
            button("Integrantes") {
                request(GatewayRequest("group-members", JSONObject().put("conversation", id))) { value ->
                    val list = value as JSONArray
                    info((0 until list.length()).joinToString(", ") { label(list.getString(it)) })
                }
            }
            if (conversation.optString("owner") == own) {
                for ((op, title) in listOf("group-add" to "Añadir integrante", "group-remove" to "Retirar integrante")) {
                    button(title) { chooseUser { user ->
                        mutate(op, JSONObject().put("conversation", id).put("member", user))
                    } }
                }
            }
        }
        if (more) button("Siguiente página") { page("history") }
        val protectedChat = conversation.optBoolean("e2ee")
        if (protectedChat) addText("Conversación cifrada")
        button("Configurar cifrado") { configureSecure(id) }
        if (protectedChat) button("Compartir claves de historial") { shareHistory(id) }
        button("Adjuntar imagen") { chooseAttachment(id, "image/png", "image/jpeg") }
        button("Adjuntar documento") { chooseAttachment(id, "*/*") }
        upload?.let { attempt ->
            button("Reanudar archivo") { sendAttachment() }
            if (!attempt.publishing) button("Cancelar archivo") {
                attempt.cancel.set(true)
                if (!mediaBusy) sendAttachment()
            }
        }
        for (message in messages.values) {
            val item = message.optJSONObject("attachment")
            if (item != null) {
                addAttachment(item, message.optJSONObject("ticket"), message.getString("conversation"),
                              message.getString("type"))
                continue
            }
            addText(label(message.getString("sender")) + "\n" + message.getString("text")).apply {
                setBackgroundColor(if (message.getString("sender") == own) 0xffccfbf1.toInt() else 0xffe2e8f0.toInt())
                setPadding(12, 8, 12, 8)
            }
        }
        val input = field("Mensaje", default = draft)
        input.addTextChangedListener(object : android.text.TextWatcher {
            override fun beforeTextChanged(value: CharSequence?, start: Int, count: Int, after: Int) {}
            override fun onTextChanged(value: CharSequence?, start: Int, before: Int, count: Int) {
                draft = value?.toString().orEmpty()
            }
            override fun afterTextChanged(value: android.text.Editable?) {}
        })
        button("Enviar") {
            if (pending != null) info("Reintenta el envío anterior")
            else if (draft.isNotBlank()) prepareSecureSend(id, draft, protectedChat)
        }
        if (pending != null) button("Reintentar envío") { send() }
    }

    private fun linkMenu() {
        val options = arrayOf("Generar QR", "Autorizar descriptor", "Consultar estado", "Cancelar vínculo")
        AlertDialog.Builder(this).setTitle("Vínculo de dispositivo").setItems(options) { _, item ->
            when (item) {
                0 -> createLink()
                1 -> approveLink()
                2, 3 -> {
                    val token = session ?: return@setItems
                    val account = own
                    val start = minimum
                    secureTask({
                        val id = secure.pending(account).readText()
                        secureCall(GatewayRequest(if (item == 2) "link-info" else "link-cancel",
                            JSONObject().put("link", id)), token, start)
                    }) { result ->
                        if (item == 3) showResult(result)
                        else {
                            info(result.getString("status"))
                            if (result.getString("status") == "LINKED") registerDevice()
                        }
                    }
                }
            }
        }.show()
    }

    private fun createLink() {
        val token = session ?: return
        val account = own
        val start = minimum
        secureTask({
            val device = checkNotNull(secure.load(account, true))
            val path = secure.pending(account)
            if (path.isFile) {
                val id = path.readText()
                val found = secureCall(GatewayRequest("link-info", JSONObject().put("link", id)), token, start)
                if (found.optString("status") == "PENDING") {
                    val qr = secureCall(GatewayRequest("link-qr", JSONObject().put("link", id)), token, start)
                    return@secureTask showableLink(qr, device)
                }
            }
            val challenge = ByteArray(32).also(java.security.SecureRandom()::nextBytes)
                .joinToString("") { "%02x".format(it.toInt() and 255) }
            val issued = (System.currentTimeMillis() / 1000).toString()
            val keys = device.keys
            val cmd = GatewayRequest("link-start", JSONObject().put("device", keys.id).put("exchange", keys.exchange)
                .put("signing", keys.signing).put("challenge", challenge).put("issued", issued))
            cmd.data.put("proof", secure.crypto.signLink(device, "start", cmd.id, token, challenge, issued))
            val result = secureCall(cmd, token, start)
            check(isApplied(result)) { "Vínculo pendiente o rechazado" }
            path.writeText(cmd.id)
            val qr = secureCall(GatewayRequest("link-qr", JSONObject().put("link", cmd.id)), token, start)
            showableLink(qr, device)
        }) { (descriptor, fingerprint, bitmap) ->
            val panel = LinearLayout(this).apply {
                orientation = LinearLayout.VERTICAL
                if (bitmap != null) addView(ImageView(this@MainActivity).apply {
                    setImageBitmap(bitmap)
                    adjustViewBounds = true
                    contentDescription = "QR público"
                })
                addView(TextView(this@MainActivity).apply {
                    text = "Huella SHA-256: $fingerprint\n$descriptor"
                    setTextIsSelectable(true)
                })
            }
            AlertDialog.Builder(this).setTitle("QR de vinculación").setView(panel)
                .setPositiveButton("Cerrar", null).show()
        }
    }

    private fun showableLink(qr: JSONObject, device: MobileE1.Device): Triple<String, String, android.graphics.Bitmap?> {
        val encoded = qr.optString("png", "")
        val bitmap = if (encoded.isBlank() || qr.isNull("png") || encoded.length > 16000) null else {
            val bytes = android.util.Base64.decode(encoded, android.util.Base64.DEFAULT)
            val opts = android.graphics.BitmapFactory.Options().apply { inJustDecodeBounds = true }
            android.graphics.BitmapFactory.decodeByteArray(bytes, 0, bytes.size, opts)
            check(opts.outWidth in 1..600 && opts.outHeight in 1..600 && opts.outMimeType == "image/png")
            android.graphics.BitmapFactory.decodeByteArray(bytes, 0, bytes.size)
        }
        return Triple(qr.getString("descriptor"), secure.crypto.fingerprint(device.keys.exchange, device.keys.signing), bitmap)
    }

    private fun approveLink() {
        val old = localIdentity ?: run { info("Activa primero tu identidad"); return }
        val input = EditText(this).apply { hint = "Pega el descriptor público leído del QR" }
        AlertDialog.Builder(this).setTitle("Autorizar vínculo").setView(input).setNegativeButton("Cancelar", null)
            .setPositiveButton("Revisar") { _, _ ->
                val token = session ?: return@setPositiveButton
                val account = own
                val start = minimum
                val descriptor = input.text.toString()
                if (descriptor.length > 512) { info("Descriptor demasiado largo"); return@setPositiveButton }
                secureTask({
                    val data = JSONObject(descriptor)
                    check(data.getInt("v") == 1)
                    val link = secureCall(GatewayRequest("link-info", JSONObject().put("link", data.getString("id"))), token, start)
                    for (field in listOf("id", "device", "challenge", "expires")) {
                        check(data.getString(field) == link.getString(field)) { "Descriptor alterado" }
                    }
                    val mark = secure.crypto.fingerprint(link.getString("exchange"), link.getString("signing"))
                    check(mark.take(32) == link.getString("device") && link.getString("status") == "PENDING") { "Vínculo no disponible" }
                    link to mark
                }) { (link, mark) ->
                    AlertDialog.Builder(this).setTitle("Compara la huella y autoriza")
                        .setMessage(mark).setNegativeButton("Cancelar", null).setPositiveButton("Autorizar") { _, _ ->
                            secureTask({
                                val observed = (System.currentTimeMillis() / 1000).toString()
                                val ident = link.getString("id")
                                val next = link.getString("device")
                                val approve = GatewayRequest("link-approve", JSONObject().put("link", ident)
                                    .put("observed", observed).put("device", old.keys.id))
                                approve.data.put("proof", secure.crypto.signLink(old, "approve", approve.id, token,
                                    ident, next, link.getString("challenge"), observed))
                                val result = secureCall(approve, token, start)
                                if (isApplied(result)) secure.pin(account, account, next).writeText(mark)
                                result
                            }) { result -> showResult(result) }
                        }.show()
                }
            }.show()
    }

    private fun devices() {
        request(GatewayRequest("devices")) { value ->
            val devices = rows(value as JSONArray).filter { it.getBoolean("active") }
            if (devices.isEmpty()) { info("No hay identidades activas"); return@request }
            AlertDialog.Builder(this).setTitle("Dispositivos activos")
                .setItems(devices.map { it.getString("id") }.toTypedArray()) { _, index ->
                    val id = devices[index].getString("id")
                    AlertDialog.Builder(this).setTitle("Revocar dispositivo")
                        .setMessage(id).setNegativeButton("Cancelar", null).setPositiveButton("Revocar") { _, _ ->
                            request(GatewayRequest("device-revoke", JSONObject().put("device", id))) { result ->
                                if (isApplied(result as JSONObject) && localIdentity?.keys?.id == id) clearSession()
                                showResult(result)
                            }
                        }.show()
                }.show()
        }
    }

    private fun shareHistory(conversation: String) {
        val token = session ?: return
        val account = own
        val device = localIdentity ?: run { info("Activa primero tu identidad"); return }
        val start = minimum
        val input = EditText(this).apply { hint = "ID del dispositivo verificado" }
        AlertDialog.Builder(this).setTitle("Compartir claves de esta conversación").setView(input)
            .setNegativeButton("Cancelar", null).setPositiveButton("Compartir") { _, _ ->
                val id = input.text.toString().trim()
                secureTask({
                    val directory = secureCall(GatewayRequest("e2ee-directory", JSONObject()
                        .put("conversation", conversation)), token, start)
                    val target = securePeers(directory.getJSONArray("devices"), account, device)
                        .firstOrNull { it.actor == account && it.keys.id == id && it.known }
                        ?: throw IllegalArgumentException("Destino no verificado")
                    var after = "0"
                    var count = 0
                    var more: Boolean
                    do {
                        val history = secureCall(GatewayRequest("history", JSONObject().put("conversation", conversation)
                            .put("after", after).put("limit", 50)), token, start)
                        for (message in rows(history.getJSONArray("messages"))) {
                            val kind = message.getString("type")
                            if (kind !in setOf("E2EE", "E2EE_FILE")) continue
                            val body = message.getString("envelope")
                            if (body.split('.')[3].split(';').none { it.startsWith(device.keys.id + ":") }) continue
                            val epoch = message.getString("epoch")
                            val mid = message.getString("id")
                            val senderId = message.getString("device")
                            val actor = message.getString("sender")
                            val sender = message.getJSONObject("sender_keys")
                            val keys = MobileE1.Keys(senderId, sender.getString("exchange"), sender.getString("signing"))
                            val mark = secure.crypto.fingerprint(keys.exchange, keys.signing)
                            if (!(actor == account && senderId == device.keys.id)) {
                                check(secure.pin(account, actor, senderId).readText() == mark) { "Huella sin verificar" }
                            }
                            val meta = MobileE1.Meta(conversation, mid, senderId, epoch, if (kind == "E2EE_FILE") "file" else "text")
                            val content = secure.crypto.exportKey(body, meta, keys, device)
                            val grant = secure.crypto.seal(content, meta.copy(device = device.keys.id, purpose = "grant"),
                                device, listOf(target.keys))
                            val result = secureCall(GatewayRequest("history-grant", JSONObject().put("message", mid)
                                .put("device", device.keys.id).put("target", target.keys.id).put("envelope", grant)), token, start)
                            if (isApplied(result)) count++
                            else check(result.optJSONObject("application")?.optString("reason") == "HISTORY_GRANT_EXISTS")
                        }
                        after = history.getString("next_cursor")
                        more = history.getBoolean("more")
                    } while (more)
                    count
                }) { count -> info("Claves compartidas · $count") }
            }.show()
    }

    private data class SecurePeer(val actor: String, val keys: MobileE1.Keys,
                                  val mark: String, val known: Boolean)

    private fun <T> secureTask(work: () -> T, done: (T) -> Unit) {
        val owner = session ?: return
        if (busy || isDestroyed) return
        busy = true
        worker.execute {
            val result = runCatching(work)
            handler.post {
                if (isDestroyed) return@post
                busy = false
                if (session == owner) result.fold(done) { info(it.message ?: "Operación E2EE interrumpida") }
            }
        }
    }

    private fun secureCall(command: GatewayRequest, token: String, start: String): JSONObject {
        val reply = network.call(command, token, start)
        check(reply.getBoolean("ok")) { reply.getString("error") }
        return reply.getJSONObject("result")
    }

    private fun registerDevice() {
        val token = session ?: return
        val account = own
        val start = minimum
        secureTask({
            val current = checkNotNull(secure.load(account, true))
            val keys = current.keys
            val command = GatewayRequest("device-register", JSONObject().put("device", keys.id)
                .put("exchange", keys.exchange).put("signing", keys.signing))
            command.data.put("proof", secure.crypto.proof(current, command.id, token))
            val result = secureCall(command, token, start)
            check(isApplied(result)) { result.optJSONObject("application")?.optString("reason") ?: "Registro pendiente" }
            current to secure.crypto.fingerprint(keys.exchange, keys.signing)
        }) { (device, mark) ->
            localIdentity = device
            AlertDialog.Builder(this).setTitle("Huella de dispositivo").setMessage(mark)
                .setPositiveButton("Cerrar", null).show()
        }
    }

    private fun securePeers(records: JSONArray, account: String, device: MobileE1.Device): List<SecurePeer> =
        rows(records).map { row ->
            val keys = MobileE1.Keys(row.getString("id"), row.getString("exchange"), row.getString("signing"))
            val mark = secure.crypto.fingerprint(keys.exchange, keys.signing)
            check(mark.take(32) == keys.id) { "Identidad alterada" }
            val actor = row.getString("user")
            val pin = secure.pin(account, actor, keys.id)
            check(!pin.exists() || pin.readText() == mark) { "Huella alterada" }
            SecurePeer(actor, keys, mark, keys.id == device.keys.id && actor == account || pin.exists())
        }

    private fun configureSecure(conversation: String) {
        val token = session ?: return
        val device = localIdentity ?: run { info("Activa la identidad E2EE desde tu cuenta"); return }
        val account = own
        val start = minimum
        secureTask({
            val directory = secureCall(GatewayRequest("e2ee-directory", JSONObject().put("conversation", conversation)), token, start)
            directory.getBoolean("enabled") to securePeers(directory.getJSONArray("devices"), account, device)
        }) { (enabled, peers) ->
            val pendingPins = peers.filterNot { it.known }
            fun activate(index: Int) {
                if (index < pendingPins.size) {
                    val peer = pendingPins[index]
                    AlertDialog.Builder(this).setTitle("Verifica la huella de " + label(peer.actor))
                        .setMessage(peer.mark + "\nCompárala por un canal independiente.")
                        .setPositiveButton("Coincide") { _, _ -> activate(index + 1) }
                        .setNegativeButton("Cancelar", null).show()
                    return
                }
                fun save() {
                    secureTask({
                        for (peer in pendingPins) {
                            val pin = secure.pin(account, peer.actor, peer.keys.id)
                            check(!pin.exists() || pin.readText() == peer.mark)
                            pin.writeText(peer.mark)
                        }
                        if (!enabled) {
                            val result = secureCall(GatewayRequest("e2ee-enable", JSONObject().put("conversation", conversation)), token, start)
                            check(isApplied(result)) { "Activación pendiente o rechazada" }
                        }
                        secure.marker(account, conversation).writeText("E1")
                    }) {
                        current?.put("e2ee", true)
                        messages.clear()
                        cursor = "0"
                        more = false
                        thread()
                        page("history")
                    }
                }
                if (enabled) save()
                else AlertDialog.Builder(this).setTitle("Activar cifrado para nuevos mensajes")
                    .setPositiveButton("Activar") { _, _ -> save() }.setNegativeButton("Cancelar", null).show()
            }
            activate(0)
        }
    }

    private fun prepareSecureSend(conversation: String, text: String, locked: Boolean) {
        val token = session ?: return
        val account = own
        val start = minimum
        val device = localIdentity
        secureTask({
            if (!locked && !secure.marker(account, conversation).exists()) {
                GatewayRequest("send", JSONObject().put("conversation", conversation).put("text", text))
            } else {
                val local = checkNotNull(device) { "Activa tu identidad E2EE" }
                val directory = secureCall(GatewayRequest("e2ee-directory", JSONObject().put("conversation", conversation)), token, start)
                check(directory.getBoolean("enabled")) { "El servidor cambió el modo de cifrado" }
                val peers = securePeers(directory.getJSONArray("devices"), account, local)
                check(peers.all { it.known }) { "Verifica las huellas antes de enviar" }
                val epoch = directory.getString("epoch")
                val command = GatewayRequest("e2ee-send", JSONObject().put("conversation", conversation)
                    .put("device", local.keys.id).put("epoch", epoch))
                val meta = MobileE1.Meta(conversation, command.id, local.keys.id, epoch)
                command.data.put("envelope", secure.crypto.seal(text, meta, local, peers.map { it.keys }))
                command
            }
        }) { command -> pending = command; send() }
    }

    private fun readProtected(page: JSONObject, account: String, device: MobileE1.Device?) {
        val local = device ?: secure.load(account, false)
        for (message in rows(page.getJSONArray("messages"))) {
            val kind = message.getString("type")
            if (kind !in setOf("E2EE", "E2EE_FILE")) continue
            val display = runCatching {
                val target = checkNotNull(local) { "Identidad no disponible" }
                val conversation = message.getString("conversation")
                secure.marker(account, conversation).writeText("E1")
                val actor = message.getString("sender")
                val meta = MobileE1.Meta(conversation, message.getString("id"), message.getString("device"),
                    message.getString("epoch"), if (kind == "E2EE_FILE") "file" else "text")
                fun sender(user: String, id: String, row: JSONObject): MobileE1.Keys {
                    val public = MobileE1.Keys(id, row.getString("exchange"), row.getString("signing"))
                    val mark = secure.crypto.fingerprint(public.exchange, public.signing)
                    check(mark.take(32) == id) { "Identidad del emisor alterada" }
                    if (!(user == account && id == target.keys.id)) {
                        val pin = secure.pin(account, user, id)
                        check(pin.isFile && pin.readText() == mark) { "Huella del emisor sin verificar" }
                    }
                    return public
                }
                val evidence = message.optJSONObject("history_key")
                val key = evidence?.let {
                    val issuer = it.getString("issuer")
                    secure.crypto.open(it.getString("envelope"), meta.copy(device = issuer, purpose = "grant"),
                        sender(account, issuer, it), target)
                }
                val plain = secure.crypto.open(message.getString("envelope"), meta,
                    sender(actor, meta.device, message.getJSONObject("sender_keys")), target, key)
                if (kind == "E2EE_FILE") {
                    val ticket = JSONObject(plain)
                    check(ticket.getString("sha256") == message.getJSONObject("attachment").getString("sha256") &&
                        AttachmentClient.validName(ticket.getString("filename"))) { "Metadatos alterados" }
                    message.put("ticket", ticket)
                    ""
                } else plain
            }.getOrElse {
                message.remove("attachment")
                "Contenido cifrado · " + (it.message ?: "Autenticidad inválida")
            }
            message.put("text", display)
        }
    }

    private fun chooseAttachment(conversation: String, vararg types: String) {
        if (mediaBusy || upload != null) { info("Reanuda o cancela la carga anterior"); return }
        selectedConversation = conversation
        val picker = Intent(Intent.ACTION_OPEN_DOCUMENT).apply {
            addCategory(Intent.CATEGORY_OPENABLE)
            type = if (types.size == 1) types[0] else "image/*"
            if (types.size > 1) putExtra(Intent.EXTRA_MIME_TYPES, types)
        }
        startActivityForResult(picker, 81)
    }

    private fun addAttachment(item: JSONObject, ticket: JSONObject?, conversation: String, type: String) {
        val view = ticket ?: item
        addText(view.getString("filename") + " · " + view.getLong("size") + " bytes")
        button("Descargar archivo") {
            if (!mediaBusy) {
                selectedDownload = Download(item, ticket, conversation)
                startActivityForResult(Intent(Intent.ACTION_CREATE_DOCUMENT).apply {
                    addCategory(Intent.CATEGORY_OPENABLE)
                    this.type = "application/octet-stream"
                    putExtra(Intent.EXTRA_TITLE, view.getString("filename"))
                }, 82)
            }
        }
        val previewable = type == "IMAGE" || (ticket != null && ticket.optString("type") in setOf("image/png", "image/jpeg"))
        if (previewable && view.getLong("size") <= 2_097_152) button("Vista previa") {
            mediaTask({ transfer, progress ->
                if (ticket == null) transfer.preview(item, progress)
                else transfer.previewEncrypted(item, ticket, conversation, secure.crypto, progress)
            }) { bitmap ->
                val image = ImageView(this).apply {
                    adjustViewBounds = true
                    setImageBitmap(bitmap)
                    contentDescription = view.getString("filename")
                }
                AlertDialog.Builder(this).setTitle("Vista previa").setView(image)
                    .setPositiveButton("Cerrar", null).show()
            }
        }
    }

    @Deprecated("Usa las acciones estándar del selector de documentos")
    override fun onActivityResult(requestCode: Int, resultCode: Int, data: Intent?) {
        super.onActivityResult(requestCode, resultCode, data)
        val ref = data?.data
        if (resultCode != RESULT_OK || ref == null || session == null) return
        if (requestCode == 81) {
            val conversation = selectedConversation ?: return
            upload = AttachmentClient.Upload(ref, conversation)
            sendAttachment()
        } else if (requestCode == 82) {
            val download = selectedDownload ?: return
            mediaTask({ transfer, progress ->
                if (download.ticket == null) transfer.download(download.item, ref, progress)
                else transfer.downloadEncrypted(download.item, download.ticket, download.conversation, ref,
                                                secure.crypto, progress)
            }) {
                info("Integridad verificada")
            }
        }
    }

    private fun sendAttachment() {
        val attempt = upload ?: return
        val account = own
        val local = localIdentity
        val locked = current?.getString("id") == attempt.conversation && current?.optBoolean("e2ee") == true
        mediaTask({ transfer, progress ->
            if (!attempt.cancel.get() && (locked || secure.marker(account, attempt.conversation).exists())) {
                val device = checkNotNull(local) { "Activa la identidad E2EE antes de adjuntar" }
                val directory = transfer.directory(attempt.conversation)
                check(directory.getBoolean("enabled")) { "El servidor cambió el modo cifrado" }
                val devices = securePeers(directory.getJSONArray("devices"), account, device)
                check(devices.all { it.known }) { "Verifica las huellas antes de adjuntar" }
                transfer.prepareEncrypted(attempt, secure.crypto, device, directory.getString("epoch"),
                                          devices.map { it.keys })
            }
            transfer.upload(attempt, progress)
        }) { outcome ->
            val phase = outcome.getString("status")
            if (phase in setOf("COMMITTED", "ABORTED", "REJECTED", "CANCELLED")) {
                attempt.staged?.delete()
                upload = null
            }
            info(if (phase == "COMMITTED") "Archivo confirmado" else phase)
            if (current != null) {
                thread()
                if (phase == "COMMITTED") page("sync")
            }
        }
    }

    private fun <T> mediaTask(
        work: (AttachmentClient, (Int, String) -> Unit) -> T, done: (T) -> Unit
    ) {
        val owner = session ?: return
        if (mediaBusy || isDestroyed) return
        mediaBusy = true
        val attempt = upload
        val transfer = AttachmentClient(contentResolver, cacheDir, network, owner, minimum)
        info("Preparando archivo…")
        mediaWorker.execute {
            val result = runCatching {
                work(transfer) { percent, status ->
                    handler.post {
                        if (active && !isDestroyed && session == owner) info("$status · $percent%")
                    }
                }
            }
            handler.post {
                mediaBusy = false
                if (isDestroyed || session != owner) {
                    attempt?.staged?.delete()
                } else {
                    result.fold(done) {
                        info(it.message ?: "Transferencia interrumpida")
                        if (current != null) thread()
                    }
                }
            }
        }
    }

    private fun page(op: String) {
        val id = current?.getString("id") ?: return
        val data = JSONObject().put("conversation", id).put("after", cursor).put("limit", 40)
        request(GatewayRequest(op, data)) { value ->
            val result = value as JSONObject
            val batch = rows(result.getJSONArray("messages"))
            val wasMore = more
            cursor = result.getString("next_cursor")
            more = result.getBoolean("more")
            for (message in batch) messages.putIfAbsent(message.getString("id"), message)
            if (batch.isNotEmpty() || wasMore != more) thread()
        }
    }

    private fun send() {
        val attempt = pending ?: return
        thread()
        info("Enviando…")
        request(attempt) { value ->
            val result = value as JSONObject
            if (showResult(result)) pending = null
            if (session != null && current != null) {
                if (isApplied(result) && attempt.data.getString("conversation") == current?.getString("id")) draft = ""
                thread()
                if (isApplied(result) && !more) page("sync")
            }
        }
    }

    private fun mutate(op: String, data: JSONObject) {
        request(GatewayRequest(op, data)) { value ->
            val result = value as JSONObject
            showResult(result)
            if (isApplied(result)) refresh()
        }
    }

    private fun request(command: GatewayRequest, success: (Any) -> Unit) {
        if (busy || isDestroyed) return
        busy = true
        val client = network
        val token = session
        val cursorAtStart = minimum
        val account = own
        val device = localIdentity
        worker.execute {
            val outcome = runCatching {
                client.call(command, token, cursorAtStart).also { reply ->
                    if (reply.getBoolean("ok") && command.operation in setOf("history", "sync")) {
                        readProtected(reply.getJSONObject("result"), account, device)
                    }
                }
            }
            handler.post {
                if (isDestroyed) return@post
                busy = false
                outcome.fold({ reply ->
                    algorithm = reply.getString("algorithm")
                    connection.text = "Conectado · " + algorithm.uppercase()
                    if (reply.getBoolean("ok")) {
                        minimum = maxOf(minimum.toLong(), reply.getString("cursor").toLong()).toString()
                        success(reply.get("result"))
                    } else {
                        val reason = reply.getString("error")
                        if (reason == "AUTH_REQUIRED") clearSession()
                        else if (command.operation in listOf("login", "register")) credentialAttempt = null
                        if (reason == "FORBIDDEN" && command.operation in listOf("sync", "history")) {
                            current = null
                            refresh()
                        }
                        info(reason)
                    }
                }, {
                    connection.text = "Desconectado"
                    info(it.message ?: "Error del gateway")
                })
            }
        }
    }

    private fun isApplied(result: JSONObject) = result.optJSONObject("application")?.optString("status") == "APPLIED"

    private fun showResult(result: JSONObject): Boolean {
        val app = result.optJSONObject("application")
        val reason = app?.optString("reason")
        val message = when {
            isApplied(result) -> "Confirmado"
            app?.optString("status") == "REJECTED" -> "Rechazado · $reason"
            result.optString("consensus") == "ABORTED" -> "Abortado"
            result.optString("consensus") == "CONFLICT" -> "Rechazado · identidad en conflicto"
            else -> { info("Pendiente o indeterminado"); return false }
        }
        if (reason == "AUTH_REQUIRED") clearSession()
        info(message)
        return true
    }

    private fun rows(array: JSONArray) = (0 until array.length()).map(array::getJSONObject)
}
