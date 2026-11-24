package com.pc4.dogmessenger

import android.util.Log
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.InetSocketAddress
import java.net.Socket

class NetworkClient(
    private val serverIp: String,
    private val serverPort: Int,
    private val claveSecreta: String // Nueva variable para el cifrado
) {

    private var socket: Socket? = null
    private var output: DataOutputStream? = null
    private var input: DataInputStream? = null

    suspend fun conectar(): Boolean = withContext(Dispatchers.IO) {
        try {
            socket = Socket()
            socket?.connect(InetSocketAddress(serverIp, serverPort), 5000)
            socket?.soTimeout = 5000

            output = DataOutputStream(socket!!.getOutputStream())
            input = DataInputStream(socket!!.getInputStream())

            Log.d("NetworkClient", "Conectado a $serverIp:$serverPort")
            return@withContext true
        } catch (e: Exception) {
            Log.e("NetworkClient", "Error de conexión: ${e.message}")
            return@withContext false
        }
    }

    suspend fun enviarMensaje(mensaje: String, miId: String): Boolean = withContext(Dispatchers.IO) {
        try {
            val requestId = ByteArray(16).also { java.util.Random().nextBytes(it) }
            val senderBytes = miId.toByteArray(Charsets.US_ASCII)

            // 1. ENCRIPTAMOS EL MENSAJE ANTES DE ENVIARLO
            val mensajeEncriptado = CryptoUtils.encriptar(mensaje, claveSecreta)
            val commandBytes = mensajeEncriptado.toByteArray(Charsets.UTF_8)

            val payloadSize = 4 + commandBytes.size
            val bodyLength = 4 + 1 + 1 + 16 + 1 + senderBytes.size + payloadSize

            output?.writeInt(bodyLength) // Prefijo

            // Header DM2
            output?.write(byteArrayOf(0x44, 0x4D, 0x32, 0x00)) // Magic DM2\0
            output?.writeByte(1) // Algoritmo Raft
            output?.writeByte(3) // Tipo CLIENT_COMMAND (0x03)
            output?.write(requestId)
            output?.writeByte(senderBytes.size)
            output?.write(senderBytes)

            // Payload CLIENT_COMMAND
            output?.writeInt(commandBytes.size)
            output?.write(commandBytes)
            output?.flush()

            Log.d("NetworkClient", "Mensaje cifrado enviado: $mensajeEncriptado")
            return@withContext true
        } catch (e: Exception) {
            Log.e("NetworkClient", "Error al enviar: ${e.message}")
            return@withContext false
        }
    }

    fun cerrar() {
        try { socket?.close() } catch (_: Exception) {}
    }
}