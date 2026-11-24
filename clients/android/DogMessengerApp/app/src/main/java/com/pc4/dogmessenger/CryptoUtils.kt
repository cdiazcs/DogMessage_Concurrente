package com.pc4.dogmessenger

import android.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

object CryptoUtils {
    private const val ALGORITHM = "AES/CBC/PKCS5Padding"
    // IV de 16 bytes (vector de inicialización). En producción debería ser aleatorio,
    // pero para esta demo usaremos uno fijo para mantenerlo simple.
    private val IV = byteArrayOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    fun encriptar(texto: String, claveSecreta: String): String {
        // AES requiere una clave de 16, 24 o 32 bytes. Forzamos 16 bytes rellenando con ceros.
        val claveBytes = ByteArray(16)
        val clavesOriginales = claveSecreta.toByteArray(Charsets.UTF_8)
        System.arraycopy(clavesOriginales, 0, claveBytes, 0, minOf(clavesOriginales.size, 16))

        val secretKey = SecretKeySpec(claveBytes, "AES")
        val cipher = Cipher.getInstance(ALGORITHM)
        cipher.init(Cipher.ENCRYPT_MODE, secretKey, IvParameterSpec(IV))

        val bytesEncriptados = cipher.doFinal(texto.toByteArray(Charsets.UTF_8))
        // Lo convertimos a Base64 para poder enviarlo como texto
        return Base64.encodeToString(bytesEncriptados, Base64.NO_WRAP)
    }
}