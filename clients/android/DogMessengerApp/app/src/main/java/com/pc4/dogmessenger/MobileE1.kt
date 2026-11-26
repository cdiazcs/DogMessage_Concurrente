package com.pc4.dogmessenger

import java.security.KeyFactory
import java.security.KeyPairGenerator
import java.security.MessageDigest
import java.security.PrivateKey
import java.security.SecureRandom
import java.security.Signature
import java.security.spec.ECGenParameterSpec
import java.security.spec.PKCS8EncodedKeySpec
import java.security.spec.X509EncodedKeySpec
import javax.crypto.Cipher
import javax.crypto.KeyAgreement
import javax.crypto.Mac
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

/** E1 portable entre JCA y Android. El adaptador Base64 no depende del nivel de SDK. */
internal class MobileE1(private val encode: (ByteArray) -> String, private val decode: (String) -> ByteArray) {
    data class Keys(val id: String, val exchange: String, val signing: String)
    data class Device(val keys: Keys, val exchange: PrivateKey, val signing: PrivateKey)
    data class Meta(val conversation: String, val message: String, val device: String, val epoch: String,
                    val purpose: String = "text") {
        fun aad(): String {
            require(listOf(conversation, message, device).all { it.matches(Regex("[0-9a-f]{32}")) })
            require(epoch.matches(Regex("[0-9a-f]{64}")) && purpose in setOf("text", "file", "grant"))
            return listOf("dog.e2ee.1", conversation, message, device, epoch, purpose).joinToString("\n")
        }
    }

    private val random = SecureRandom()
    private fun random(size: Int) = ByteArray(size).also(random::nextBytes)
    private fun bytes(value: String) = value.toByteArray(Charsets.UTF_8)
    private fun sha(data: ByteArray) = MessageDigest.getInstance("SHA-256").digest(data)
    private fun hex(raw: ByteArray) = raw.joinToString("") { "%02x".format(it.toInt() and 255) }
    private fun unhex(text: String) = text.chunked(2).map { it.toInt(16).toByte() }.toByteArray()

    fun fingerprint(exchange: String, signing: String) = hex(sha(decode(exchange) + decode(signing)))

    fun generate(): Device {
        val generator = KeyPairGenerator.getInstance("EC").apply { initialize(ECGenParameterSpec("secp256r1")) }
        val dh = generator.generateKeyPair()
        val sign = generator.generateKeyPair()
        val x = encode(dh.public.encoded)
        val s = encode(sign.public.encoded)
        return Device(Keys(fingerprint(x, s).take(32), x, s), dh.private, sign.private)
    }

    fun restore(x: String, s: String, dh: String, sig: String): Device {
        val factory = KeyFactory.getInstance("EC")
        return Device(Keys(fingerprint(x, s).take(32), x, s),
            factory.generatePrivate(PKCS8EncodedKeySpec(decode(dh))),
            factory.generatePrivate(PKCS8EncodedKeySpec(decode(sig))))
    }

    private fun cipher(mode: Int, key: ByteArray, nonce: ByteArray, aad: String, data: ByteArray): ByteArray =
        Cipher.getInstance("AES/GCM/NoPadding").run {
            init(mode, SecretKeySpec(key, "AES"), GCMParameterSpec(128, nonce))
            updateAAD(bytes(aad))
            doFinal(data)
        }

    private fun hmac(key: ByteArray, data: ByteArray): ByteArray = Mac.getInstance("HmacSHA256").run {
        init(SecretKeySpec(key, "HmacSHA256"))
        doFinal(data)
    }

    private fun wrapping(privateKey: PrivateKey, publicKey: String, aad: String, recipient: String): ByteArray {
        val pub = KeyFactory.getInstance("EC").generatePublic(X509EncodedKeySpec(decode(publicKey)))
        val agreed = KeyAgreement.getInstance("ECDH").run {
            init(privateKey)
            doPhase(pub, true)
            generateSecret()
        }
        val prk = hmac(sha(bytes(aad)), agreed)
        return try { hmac(prk, bytes("dog.wrap.1\n$recipient") + byteArrayOf(1)) }
        finally { agreed.fill(0); prk.fill(0) }
    }

    private fun toDer(raw: ByteArray): ByteArray {
        require(raw.size == 64) { "Firma inválida" }
        fun integer(value: ByteArray): ByteArray {
            val start = value.indexOfFirst { it != 0.toByte() }
            val payload = if (start < 0) byteArrayOf(0) else value.copyOfRange(start, value.size)
            val guarded = if (payload[0] < 0) byteArrayOf(0) + payload else payload
            return byteArrayOf(2, guarded.size.toByte()) + guarded
        }
        val body = integer(raw.copyOfRange(0, 32)) + integer(raw.copyOfRange(32, 64))
        return byteArrayOf(48, body.size.toByte()) + body
    }

    private fun fromDer(raw: ByteArray): ByteArray {
        require(raw.size >= 8 && raw[0] == 48.toByte() && raw[1].toInt() == raw.size - 2)
        val out = ByteArray(64)
        var offset = 2
        for (index in 0..1) {
            require(offset + 2 <= raw.size && raw[offset] == 2.toByte())
            val original = raw[offset + 1].toInt()
            offset += 2
            require(original in 1..33 && offset + original <= raw.size && raw[offset] >= 0)
            val skip = if (raw[offset] == 0.toByte() && original > 1) 1 else 0
            val size = original - skip
            require(size <= 32)
            raw.copyInto(out, (index + 1) * 32 - size, offset + skip, offset + original)
            offset += original
        }
        require(offset == raw.size)
        return out
    }

    private fun sign(key: PrivateKey, text: String): ByteArray = Signature.getInstance("SHA256withECDSA").run {
        initSign(key, random)
        update(bytes(text))
        fromDer(sign())
    }

    fun proof(device: Device, request: String, token: String) = encode(sign(device.signing,
        listOf("dog.device.1", request, hex(sha(unhex(token))), device.keys.id).joinToString("\n")))

    fun seal(text: String, meta: Meta, sender: Device, recipients: List<Keys>): String {
        val aad = meta.aad()
        require(text.isNotBlank() && bytes(text).size <= 4096 && recipients.size in 1..16)
        val key = random(32)
        val nonce = random(12)
        return try {
            val body = cipher(Cipher.ENCRYPT_MODE, key, nonce, aad, bytes(text))
            val entries = recipients.sortedBy { it.id }.joinToString(";") { peer ->
                val iv = random(12)
                val kek = wrapping(sender.exchange, peer.exchange, aad, peer.id)
                try { listOf(peer.id, encode(iv), encode(cipher(Cipher.ENCRYPT_MODE, kek, iv, "$aad\n${peer.id}", key))).joinToString(":") }
                finally { kek.fill(0) }
            }
            val payload = listOf("E1", encode(nonce), encode(body), entries).joinToString(".")
            "$payload.${encode(sign(sender.signing, "$aad\n$payload"))}"
        } finally { key.fill(0) }
    }

    fun signLink(device: Device, action: String, request: String, token: String, vararg fields: String): String =
        encode(sign(device.signing, (listOf("dog.link.$action.1", request, hex(sha(unhex(token))), device.keys.id)
            + fields).joinToString("\n")))

    private fun length(size: Long): Long {
        require(size in 1..52_428_800)
        val cipherSize = 20 + size + 16 * ((size + 8191) / 8192)
        require(cipherSize <= 52_428_800) { "El archivo cifrado supera 50 MiB" }
        return cipherSize
    }

    private fun fileAad(conversation: String, id: String, size: Long, index: Int): String {
        require(conversation.matches(Regex("[0-9a-f]{32}")) && id.matches(Regex("[0-9a-f]{32}")))
        return listOf("dog.file.1", conversation, id, size.toString(), index.toString()).joinToString("\n")
    }

    private fun fileNonce(prefix: ByteArray, index: Int) = java.nio.ByteBuffer.allocate(12).put(prefix).putInt(index).array()

    fun sealFile(input: java.io.InputStream, output: java.io.OutputStream, size: Long,
                 conversation: String, id: String, cancelled: () -> Boolean): String {
        length(size)
        val source = java.io.DataInputStream(input)
        val target = java.io.DataOutputStream(output)
        val key = random(32)
        val prefix = random(8)
        try {
            target.writeInt(0x44474631)
            target.writeLong(size)
            target.write(prefix)
            var offset = 0L
            while (offset < size) {
                if (cancelled() || Thread.currentThread().isInterrupted) throw InterruptedException()
                val block = ByteArray(minOf(8192L, size - offset).toInt())
                source.readFully(block)
                val index = (offset / 8192).toInt()
                target.write(cipher(Cipher.ENCRYPT_MODE, key, fileNonce(prefix, index),
                                    fileAad(conversation, id, size, index), block))
                offset += block.size
            }
            check(source.read() == -1) { "Cambió el tamaño del archivo durante la lectura" }
            return encode(key)
        } finally { key.fill(0) }
    }

    fun openFile(input: java.io.InputStream, output: java.io.OutputStream, encryptedSize: Long,
                 expectedSize: Long, secret: String, conversation: String, id: String) {
        require(encryptedSize == length(expectedSize)) { "Longitud cifrada inválida" }
        val source = java.io.DataInputStream(input)
        val key = decode(secret)
        try {
            require(key.size == 32 && source.readInt() == 0x44474631 && source.readLong() == expectedSize)
            val prefix = ByteArray(8)
            source.readFully(prefix)
            var offset = 0L
            while (offset < expectedSize) {
                if (Thread.currentThread().isInterrupted) throw InterruptedException()
                val count = minOf(8192L, expectedSize - offset).toInt()
                val block = ByteArray(count + 16)
                source.readFully(block)
                val index = (offset / 8192).toInt()
                output.write(cipher(Cipher.DECRYPT_MODE, key, fileNonce(prefix, index),
                                    fileAad(conversation, id, expectedSize, index), block))
                offset += count
            }
            require(source.read() == -1) { "Archivo cifrado con datos sobrantes" }
        } finally { key.fill(0) }
    }

    fun open(box: String, meta: Meta, sender: Keys, target: Device, secret: String? = null): String =
        openVerified(box, meta, sender, target, secret, false)

    fun exportKey(box: String, meta: Meta, sender: Keys, target: Device): String =
        openVerified(box, meta, sender, target, null, true)

    private fun openVerified(box: String, meta: Meta, sender: Keys, target: Device,
                             secret: String?, export: Boolean): String {
        require(box.length <= 8192 && fingerprint(sender.exchange, sender.signing).take(32) == meta.device)
        val parts = box.split('.')
        require(parts.size == 5 && parts[0] == "E1")
        val aad = meta.aad()
        val publicKey = KeyFactory.getInstance("EC").generatePublic(X509EncodedKeySpec(decode(sender.signing)))
        val valid = Signature.getInstance("SHA256withECDSA").run {
            initVerify(publicKey)
            update(bytes("$aad\n${parts.take(4).joinToString(".")}"))
            verify(toDer(decode(parts[4])))
        }
        require(valid) { "Firma del mensaje inválida" }
        val key = if (secret != null) decode(secret) else {
            val wrap = parts[3].split(';').map { it.split(':') }
                .firstOrNull { it.size == 3 && it[0] == target.keys.id }
                ?: throw IllegalArgumentException("Este dispositivo no recibió la clave")
            val kek = wrapping(target.exchange, sender.exchange, aad, target.keys.id)
            try { cipher(Cipher.DECRYPT_MODE, kek, decode(wrap[1]), "$aad\n${target.keys.id}", decode(wrap[2])) }
            finally { kek.fill(0) }
        }
        return try {
            require(key.size == 32)
            val bytes = cipher(Cipher.DECRYPT_MODE, key, decode(parts[1]), aad, decode(parts[2]))
            if (export) encode(key) else bytes.toString(Charsets.UTF_8)
        } finally { key.fill(0) }
    }
}
