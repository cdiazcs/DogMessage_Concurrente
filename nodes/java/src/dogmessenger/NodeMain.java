package dogmessenger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Nodo TCP base interoperable (todavía NO implementa Raft). Solo JDK. */
public final class NodeMain {
    private static final int MAX_FRAME = 1024 * 1024;
    private static final String ID_PATTERN = "[a-zA-Z0-9_-]{1,32}";
    private static final String REQUEST_PATTERN = "[a-fA-F0-9]{32}";

    private static void sendFrame(DataOutputStream out, String body) throws IOException {
        byte[] payload = body.getBytes(StandardCharsets.UTF_8);
        if (payload.length == 0 || payload.length > MAX_FRAME) throw new IOException("Longitud de trama inválida");
        out.writeInt(payload.length); // Big endian
        out.write(payload);
        out.flush();
    }

    private static String readFrame(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length <= 0 || length > MAX_FRAME) throw new IOException("Longitud de trama inválida");
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void handle(Socket socket, String nodeId) {
        try (socket;
             DataInputStream in = new DataInputStream(socket.getInputStream());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            socket.setSoTimeout(5000);
            String[] parts = readFrame(in).split("\\|", -1);
            if (parts.length != 4 || !parts[0].equals("DM1") || !parts[1].equals("PING")
                || !parts[2].matches(ID_PATTERN) || !parts[3].matches(REQUEST_PATTERN)) {
                sendFrame(out, "DM1|ERROR|" + nodeId + "|00000000000000000000000000000000");
                return;
            }
            sendFrame(out, "DM1|PONG|" + nodeId + "|" + parts[3]);
        } catch (IOException ignored) {
            // Un cliente desconectado no debe detener el nodo.
        }
    }

    private static void serve(String nodeId, String host, int port) throws IOException {
        try (ServerSocket server = new ServerSocket()) {
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress(host, port));
            ExecutorService pool = Executors.newFixedThreadPool(16);
            System.out.println("[JAVA " + nodeId + "] LISTEN " + host + ":" + port + " protocol=DM1");
            while (true) {
                Socket client = server.accept();
                pool.execute(() -> handle(client, nodeId));
            }
        }
    }

    private static int ping(String nodeId, String endpoint) throws IOException {
        int idx = endpoint.lastIndexOf(':');
        if (idx <= 0) throw new IOException("Usa --ping IP:PUERTO");
        String host = endpoint.substring(0, idx);
        int port = Integer.parseInt(endpoint.substring(idx + 1));
        String requestId = UUID.randomUUID().toString().replace("-", "");
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 5000);
            socket.setSoTimeout(5000);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());
            sendFrame(out, "DM1|PING|" + nodeId + "|" + requestId);
            String[] parts = readFrame(in).split("\\|", -1);
            if (parts.length == 4 && "DM1".equals(parts[0]) && "PONG".equals(parts[1])
                && parts[2].matches(ID_PATTERN) && requestId.equals(parts[3])) {
                System.out.println("OK " + nodeId + " -> " + endpoint + ": PONG de " + parts[2]);
                return 0;
            }
            System.err.println("ERROR respuesta inesperada");
            return 1;
        }
    }

    public static void main(String[] args) {
        String id = null, host = "0.0.0.0", target = null;
        int port = 9101;
        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--id": id = args[++i]; break;
                    case "--host": host = args[++i]; break;
                    case "--port": port = Integer.parseInt(args[++i]); break;
                    case "--ping": target = args[++i]; break;
                    default: throw new IllegalArgumentException("Argumento desconocido " + args[i]);
                }
            }
            if (id == null || !id.matches(ID_PATTERN)) throw new IllegalArgumentException("--id obligatorio (1-32: letras, números, _ o -)");
            if (target == null) serve(id, host, port);
            else System.exit(ping(id, target));
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            System.exit(1);
        }
    }
}
