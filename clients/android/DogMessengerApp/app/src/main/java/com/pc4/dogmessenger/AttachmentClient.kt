package com.pc4.dogmessenger

import android.content.ContentResolver
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.net.Uri
import android.provider.OpenableColumns
import android.util.Base64
import org.json.JSONObject
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicBoolean

internal class AttachmentClient(
    private val resolver: ContentResolver,
    private val cache: File,
    private val network: NetworkClient,
    private val session: String,
    private val cursor: String
) {
    class Upload(val uri: Uri, val conversation: String) {
        val cancel = AtomicBoolean()
        var staged: File? = null
        var init: GatewayRequest? = null
        var finish: GatewayRequest? = null
        @Volatile var publishing = false
    }

    private fun call(request: GatewayRequest): JSONObject {
        if (Thread.currentThread().isInterrupted) throw InterruptedException()
        val reply = network.call(request, session, cursor)
        if (!reply.getBoolean("ok")) throw IOException(reply.getString("error"))
        return reply.getJSONObject("result")
    }

    private fun call(op: String, data: JSONObject) = call(GatewayRequest(op, data))

    private fun prepare(state: Upload) {
        val column = arrayOf(OpenableColumns.DISPLAY_NAME)
        val name = resolver.query(state.uri, column, null, null, null)?.use {
            if (it.moveToFirst() && !it.isNull(0)) it.getString(0) else null
        } ?: "archivo"
        val file = File.createTempFile("upload-", ".part", cache)
        val hash = MessageDigest.getInstance("SHA-256")
        var size = 0L
        try {
            resolver.openInputStream(state.uri)?.use { input ->
                file.outputStream().use { output ->
                    val block = ByteArray(64 * 1024)
                    while (true) {
                        if (Thread.currentThread().isInterrupted || state.cancel.get()) throw InterruptedException()
                        val count = input.read(block)
                        if (count < 0) break
                        size += count
                        require(size <= LIMIT) { "Archivo mayor de 50 MiB" }
                        hash.update(block, 0, count)
                        output.write(block, 0, count)
                    }
                }
            } ?: throw IOException("El proveedor no abrió el archivo")
            require(size > 0) { "Archivo vacío" }
            val init = GatewayRequest("upload-init", JSONObject().put("conversation", state.conversation)
                .put("filename", name).put("size", size).put("sha256", hex(hash.digest()))
                .put("content_type", "application/octet-stream"))
            state.staged = file
            state.init = init
            state.finish = GatewayRequest("upload-complete", JSONObject().put("transfer", init.id))
        } finally {
            if (state.staged == null) file.delete()
        }
    }

    fun directory(conversation: String): JSONObject =
        call("e2ee-directory", JSONObject().put("conversation", conversation))

    fun prepareEncrypted(state: Upload, crypto: MobileE1, device: MobileE1.Device, epoch: String,
                         recipients: List<MobileE1.Keys>) {
        if (state.init != null) return
        val name = resolver.query(state.uri, arrayOf(OpenableColumns.DISPLAY_NAME), null, null, null)?.use {
            if (it.moveToFirst() && !it.isNull(0)) it.getString(0) else null
        } ?: "archivo"
        require(validName(name)) { "Nombre de archivo inválido" }
        val type = resolver.getType(state.uri).takeIf { it in setOf("image/png", "image/jpeg") }
            ?: "application/octet-stream"
        val size = resolver.openInputStream(state.uri)?.use { source ->
            val buffer = ByteArray(64 * 1024)
            var count = 0L
            while (true) {
                if (Thread.currentThread().isInterrupted || state.cancel.get()) throw InterruptedException()
                val read = source.read(buffer)
                if (read < 0) break
                count += read
                require(count <= LIMIT) { "Archivo mayor de 50 MiB" }
            }
            count
        } ?: throw IOException("El proveedor no abrió el archivo")
        val id = java.util.UUID.randomUUID().toString().replace("-", "")
        val sealed = File.createTempFile("upload-dgf-", ".bin", cache)
        try {
            val hash = MessageDigest.getInstance("SHA-256")
            val key = resolver.openInputStream(state.uri)?.use { source ->
                sealed.outputStream().use { output ->
                    crypto.sealFile(source, java.security.DigestOutputStream(output, hash), size, state.conversation,
                                    id, state.cancel::get)
                }
            } ?: throw IOException("El proveedor no reabrió el archivo")
            val sha = hex(hash.digest())
            val ticket = JSONObject().put("filename", name).put("size", size).put("sha256", sha)
                .put("type", type).put("key", key)
            val envelope = crypto.seal(ticket.toString(), MobileE1.Meta(state.conversation, id, device.keys.id,
                epoch, "file"), device, recipients)
            val init = GatewayRequest("upload-encrypted-init", JSONObject().put("conversation", state.conversation)
                .put("message", id).put("device", device.keys.id).put("epoch", epoch)
                .put("size", sealed.length()).put("sha256", sha).put("envelope", envelope))
            state.finish = GatewayRequest("upload-complete", JSONObject().put("transfer", init.id), id)
            state.staged = sealed
            state.init = init
        } finally { if (state.init == null) sealed.delete() }
    }

    fun downloadEncrypted(item: JSONObject, ticket: JSONObject, conversation: String, target: Uri, crypto: MobileE1,
                          progress: (Int, String) -> Unit) {
        val sealed = verifiedFile(item, progress)
        try {
            fun copy(output: java.io.OutputStream) {
                sealed.inputStream().use { input ->
                    crypto.openFile(input, output, sealed.length(), ticket.getLong("size"), ticket.getString("key"),
                                    conversation, item.getString("id"))
                }
            }
            copy(object : java.io.OutputStream() {
                override fun write(value: Int) {}
                override fun write(bytes: ByteArray, offset: Int, count: Int) {}
            })
            resolver.openOutputStream(target, "wt")?.use(::copy) ?: throw IOException("El proveedor no abrió el destino")
        } finally { sealed.delete() }
    }

    fun previewEncrypted(item: JSONObject, ticket: JSONObject, conversation: String, crypto: MobileE1,
                         progress: (Int, String) -> Unit): Bitmap {
        require(ticket.getString("type") in setOf("image/png", "image/jpeg") &&
            ticket.getLong("size") in 1..2_097_152)
        val sealed = verifiedFile(item, progress)
        try {
            val out = java.io.ByteArrayOutputStream()
            sealed.inputStream().use { input ->
                crypto.openFile(input, out, sealed.length(), ticket.getLong("size"), ticket.getString("key"),
                                conversation, item.getString("id"))
            }
            val bytes = out.toByteArray()
            val options = BitmapFactory.Options().apply { inJustDecodeBounds = true }
            BitmapFactory.decodeByteArray(bytes, 0, bytes.size, options)
            require(options.outMimeType == ticket.getString("type") && options.outWidth > 0 && options.outHeight > 0 &&
                options.outWidth.toLong() * options.outHeight <= 8_000_000) { "Imagen fuera del límite" }
            options.inJustDecodeBounds = false
            return BitmapFactory.decodeByteArray(bytes, 0, bytes.size, options) ?: throw IOException("Imagen no válida")
        } finally { sealed.delete() }
    }

    fun upload(state: Upload, progress: (Int, String) -> Unit): JSONObject {
        if (state.init == null) {
            if (state.cancel.get()) return JSONObject().put("status", "CANCELLED")
            try {
                prepare(state)
            } catch (error: InterruptedException) {
                if (state.cancel.get()) return JSONObject().put("status", "CANCELLED")
                throw error
            }
        }
        val init = checkNotNull(state.init)
        val file = checkNotNull(state.staged)
        val reference = JSONObject().put("transfer", init.id)
        call(init)
        val status = call("upload-status", reference)
        val size = file.length()
        if (!state.publishing) {
            val blockSize = status.getInt("block_size")
            require(blockSize == BLOCK)
            var offset = status.getLong("received")
            RandomAccessFile(file, "r").use { stream ->
                stream.seek(offset)
                while (offset < size && !state.cancel.get()) {
                    val data = ByteArray(minOf(blockSize.toLong(), size - offset).toInt())
                    stream.readFully(data)
                    val reply = call("upload-chunk", JSONObject().put("transfer", init.id)
                        .put("offset", offset).put("block", Base64.encodeToString(data, Base64.NO_WRAP)))
                    require(reply.getLong("received") == offset + data.size) { "Bloque no confirmado" }
                    offset += data.size
                    progress((100 * offset / size).toInt(), "Enviando bloques")
                }
            }
            if (state.cancel.get()) return call("upload-cancel", reference)
            state.publishing = true
        }
        var reply = JSONObject().put("status", "PENDING")
        repeat(24) {
            reply = call(checkNotNull(state.finish))
            val phase = reply.getString("status")
            progress(100, phase)
            if (phase !in setOf("REPLICATING", "PENDING")) return reply
            Thread.sleep(750)
        }
        return reply
    }

    private fun verifiedFile(item: JSONObject, progress: (Int, String) -> Unit): File {
        val id = item.getString("id")
        val size = item.getLong("size")
        require(size in 1..LIMIT) { "Tamaño fuera del contrato" }
        var ready = false
        for (i in 0 until 24) {
            val result = call("download-init", JSONObject().put("attachment", id))
            if (result.getString("status") == "READY") {
                require(result.getInt("block_size") == BLOCK)
                ready = true
                break
            }
            Thread.sleep(500)
        }
        if (!ready) throw IOException("Contenido temporalmente inaccesible")
        val file = File.createTempFile("download-", ".part", cache)
        val hash = MessageDigest.getInstance("SHA-256")
        var complete = false
        try {
            file.outputStream().use { output ->
                var offset = 0L
                while (offset < size) {
                    val reply = call("download-chunk", JSONObject().put("attachment", id).put("offset", offset))
                    val data = Base64.decode(reply.getString("block"), Base64.DEFAULT)
                    require(reply.getLong("offset") == offset && data.size.toLong() == minOf(BLOCK.toLong(), size - offset)) {
                        "Bloque incompleto"
                    }
                    hash.update(data)
                    output.write(data)
                    offset += data.size
                    progress((100 * offset / size).toInt(), "Descargando bloques")
                }
            }
            require(hex(hash.digest()) == item.getString("sha256")) { "Integridad SHA-256 incorrecta" }
            complete = true
            return file
        } finally {
            if (!complete) file.delete()
        }
    }

    fun download(item: JSONObject, destination: Uri, progress: (Int, String) -> Unit) {
        val file = verifiedFile(item, progress)
        try {
            resolver.openOutputStream(destination, "wt")?.use { output ->
                file.inputStream().use { it.copyTo(output, 64 * 1024) }
            } ?: throw IOException("El proveedor no abrió el destino")
        } finally {
            file.delete()
        }
    }

    fun preview(item: JSONObject, progress: (Int, String) -> Unit): Bitmap {
        require(item.getString("content_type") in setOf("image/png", "image/jpeg") &&
            item.getLong("size") <= 2_097_152)
        val file = verifiedFile(item, progress)
        try {
            val opts = BitmapFactory.Options().apply { inJustDecodeBounds = true }
            BitmapFactory.decodeFile(file.path, opts)
            require(opts.outWidth > 0 && opts.outHeight > 0 &&
                opts.outWidth.toLong() * opts.outHeight <= 8_000_000) { "Dimensiones fuera del límite" }
            opts.inJustDecodeBounds = false
            opts.inSampleSize = 2
            return BitmapFactory.decodeFile(file.path, opts) ?: throw IOException("Imagen no válida")
        } finally {
            file.delete()
        }
    }

    private fun hex(data: ByteArray) = data.joinToString("") { "%02x".format(it.toInt() and 255) }

    companion object {
        fun validName(name: String) = name.isNotBlank() && name !in setOf(".", "..")
            && name.toByteArray(Charsets.UTF_8).size <= 240
            && name.none { it in "/\\:" || it.code < 32 || it.code in 127..159 ||
                          it.code in 0x202a..0x202e || it.code in 0x2066..0x2069 }
        private const val BLOCK = 8192
        private const val LIMIT = 50L * 1024 * 1024
    }
}
