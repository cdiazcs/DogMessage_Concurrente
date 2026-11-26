package com.pc4.dogmessenger

import org.json.JSONObject
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.net.InetSocketAddress
import java.net.Socket
import java.nio.ByteBuffer
import java.util.UUID
import javax.net.ssl.SSLSocket
import javax.net.ssl.SSLSocketFactory

internal data class GatewayRequest(
    val operation: String,
    val data: JSONObject = JSONObject(),
    val id: String = UUID.randomUUID().toString().replace("-", "")
)

internal class NetworkClient(private val host: String, private val port: Int) {
    fun call(request: GatewayRequest, token: String?, minimum: String): JSONObject {
        val payload = JSONObject().put("v", 1).put("id", request.id).put("op", request.operation)
            .put("data", request.data).put("min_cursor", minimum)
        if (token != null) payload.put("session", token)
        val body = payload.toString().toByteArray(Charsets.UTF_8)
        require(body.size <= 16_384) { "Solicitud demasiado grande" }
        Socket().use { tcp ->
            tcp.connect(InetSocketAddress(host, port), 5000)
            val factory = SSLSocketFactory.getDefault() as SSLSocketFactory
            (factory.createSocket(tcp, host, port, true) as SSLSocket).use { tls ->
                tls.sslParameters = tls.sslParameters.apply { endpointIdentificationAlgorithm = "HTTPS" }
                tls.soTimeout = 15_000
                tls.startHandshake()
                val output = DataOutputStream(tls.outputStream)
                output.writeInt(body.size)
                output.write(body)
                output.flush()
                val input = DataInputStream(tls.inputStream)
                val length = input.readInt()
                if (length !in 1..2_097_152) throw IOException("Longitud de respuesta inválida")
                val raw = ByteArray(length)
                input.readFully(raw)
                val text = Charsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(raw)).toString()
                val reply = JSONObject(text)
                if (reply.getInt("v") != 1 || reply.getString("id") != request.id) {
                    throw IOException("Respuesta fuera de contrato")
                }
                return reply
            }
        }
    }
}
