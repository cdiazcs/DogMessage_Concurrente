import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

final class NetworkClient {
    private final String host;
    private final int port;

    NetworkClient(String host, int port) {
        if (host.isBlank() || port < 1 || port > 65535) throw new IllegalArgumentException("Dirección inválida");
        this.host = host;
        this.port = port;
    }

    record Request(String op, Map<String, Object> data, String id) {
        Request(String op, Map<String, Object> data) {
            this(op, Map.copyOf(data), UUID.randomUUID().toString().replace("-", ""));
        }
    }

    Map<String, Object> call(Request request, String session, String cursor) throws IOException {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("v", 1);
        document.put("id", request.id());
        document.put("op", request.op());
        document.put("data", request.data());
        document.put("min_cursor", cursor);
        if (session != null) document.put("session", session);
        byte[] body = Json.encode(document).getBytes(StandardCharsets.UTF_8);
        if (body.length > 16_384) throw new IOException("Solicitud demasiado grande");

        try (Socket tcp = new Socket()) {
            tcp.connect(new InetSocketAddress(host, port), 5_000);
            SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
            try (SSLSocket tls = (SSLSocket) factory.createSocket(tcp, host, port, true)) {
                var parameters = tls.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                tls.setSSLParameters(parameters);
                tls.setSoTimeout(15_000);
                tls.startHandshake();
                DataOutputStream out = new DataOutputStream(tls.getOutputStream());
                out.writeInt(body.length);
                out.write(body);
                out.flush();
                DataInputStream in = new DataInputStream(tls.getInputStream());
                int length = in.readInt();
                if (length < 1 || length > 2_097_152) throw new IOException("Longitud de respuesta inválida");
                byte[] raw = new byte[length];
                in.readFully(raw);
                var response = Json.map(Json.decode(StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(raw)).toString()));
                if (!request.id().equals(response.get("id")) || !Long.valueOf(1).equals(response.get("v"))) {
                    throw new IOException("Respuesta fuera de contrato");
                }
                return response;
            }
        }
    }
}
