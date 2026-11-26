package com.pc4.dogmessenger

import android.content.Context
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import android.util.AtomicFile
import android.util.Base64
import org.json.JSONObject
import java.io.File
import java.security.KeyStore
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec

internal class AndroidIdentity(context: Context) {
    private val directory = File(context.noBackupFilesDir, "dog-e2ee")
    private val encode: (ByteArray) -> String = { Base64.encodeToString(it, Base64.NO_WRAP) }
    private val decode: (String) -> ByteArray = { Base64.decode(it, Base64.DEFAULT) }
    val crypto = MobileE1(encode, decode)

    private fun account(id: String): File {
        require(id.matches(Regex("[0-9a-f]{32}")))
        return File(directory, id)
    }

    private fun wrappingKey(user: String, existing: Boolean): SecretKey {
        val alias = "dog-messenger-e1-$user"
        val store = KeyStore.getInstance("AndroidKeyStore").apply { load(null) }
        (store.getKey(alias, null) as? SecretKey)?.let { return it }
        check(!existing) { "La clave Android Keystore no está disponible" }
        return KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, "AndroidKeyStore").run {
            init(KeyGenParameterSpec.Builder(alias, KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT)
                .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
                .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                .setKeySize(256)
                .build())
            generateKey()
        }
    }

    fun load(user: String, create: Boolean): MobileE1.Device? {
        val root = account(user)
        val path = File(root, "device.bin")
        val file = AtomicFile(path)
        val aad = "dog.android.device.1\n$user".toByteArray(Charsets.UTF_8)
        if (path.exists()) {
            check(path.length() in 29..4096) { "Identidad local inválida" }
            val data = file.readFully()
            val cipher = Cipher.getInstance("AES/GCM/NoPadding")
            cipher.init(Cipher.DECRYPT_MODE, wrappingKey(user, true), GCMParameterSpec(128, data.copyOfRange(0, 12)))
            cipher.updateAAD(aad)
            val json = JSONObject(cipher.doFinal(data.copyOfRange(12, data.size)).toString(Charsets.UTF_8))
            return crypto.restore(json.getString("exchange"), json.getString("signing"),
                                  json.getString("dh"), json.getString("sign"))
        }
        if (!create) return null
        check(root.mkdirs() || root.isDirectory)
        val device = crypto.generate()
        val payload = JSONObject().put("exchange", device.keys.exchange).put("signing", device.keys.signing)
            .put("dh", encode(device.exchange.encoded)).put("sign", encode(device.signing.encoded))
        val cipher = Cipher.getInstance("AES/GCM/NoPadding")
        cipher.init(Cipher.ENCRYPT_MODE, wrappingKey(user, false))
        check(cipher.iv.size == 12)
        cipher.updateAAD(aad)
        val bytes = cipher.iv + cipher.doFinal(payload.toString().toByteArray(Charsets.UTF_8))
        val output = file.startWrite()
        try {
            output.write(bytes)
            file.finishWrite(output)
        } catch (error: Exception) {
            file.failWrite(output)
            throw error
        }
        return device
    }

    fun pin(user: String, actor: String, device: String) =
        File(account(user), "pin-${identifier(actor)}-${identifier(device)}")

    fun pending(user: String) = File(account(user), "link-id")

    fun marker(user: String, conversation: String) = File(account(user), "locked-${identifier(conversation)}")

    private fun identifier(value: String): String {
        require(value.matches(Regex("[0-9a-f]{32}")))
        return value
    }
}
