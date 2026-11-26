package dogmessenger.twopc;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;

public final class Wire {
    public static final int ALGORITHM = 2, SUBMIT = 1, PREPARE = 2, COMMIT = 3, ABORT = 4, QUERY = 5;
    public static final int MAX_BODY = 1_048_576, MAX_COMMAND = 65_536;
    private static final int MAGIC = 0x444d3200;

    public record Header(int algorithm, int kind, UUID token, String sender) {}
    public record Frame(Header header, DataInputStream input) {}
    public record Tx(long sequence, UUID id, String coordinator, String client, UUID request, byte[] bytes) {
        public boolean matches(Tx other) {
            return sequence == other.sequence && id.equals(other.id) && coordinator.equals(other.coordinator)
                    && client.equals(other.client) && request.equals(other.request) && Arrays.equals(bytes, other.bytes);
        }
    }
    public record Reference(long sequence, UUID id) {}
    public record Reply(Reference reference, int status) {}
    public record Result(int status, long sequence, String coordinator) {}
    @FunctionalInterface public interface Writer { void write(DataOutputStream out) throws IOException; }

    private Wire() {}

    public static void require(boolean condition) throws IOException {
        if (!condition) throw new IOException("Trama 2PC inválida");
    }

    public static boolean validId(String id) { return id != null && id.matches("[A-Za-z0-9_-]{1,32}"); }

    public static UUID uuid(DataInputStream in) throws IOException {
        var id = new UUID(in.readLong(), in.readLong());
        require(id.getMostSignificantBits() != 0 || id.getLeastSignificantBits() != 0);
        return id;
    }

    public static void uuid(DataOutputStream out, UUID id) throws IOException {
        require(id.getMostSignificantBits() != 0 || id.getLeastSignificantBits() != 0);
        out.writeLong(id.getMostSignificantBits());
        out.writeLong(id.getLeastSignificantBits());
    }

    public static String node(DataInputStream in) throws IOException {
        int size = in.readUnsignedByte();
        byte[] value = new byte[size];
        in.readFully(value);
        String id = new String(value, StandardCharsets.US_ASCII);
        require(validId(id));
        return id;
    }

    public static void node(DataOutputStream out, String id) throws IOException {
        require(validId(id));
        out.writeByte(id.length());
        out.write(id.getBytes(StandardCharsets.US_ASCII));
    }

    public static long sequence(DataInputStream in, boolean zero) throws IOException {
        long value = in.readLong();
        require(value >= (zero ? 0 : 1));
        return value;
    }

    public static byte[] command(DataInputStream in) throws IOException {
        int size = in.readInt();
        require(size > 0 && size <= MAX_COMMAND);
        byte[] value = new byte[size];
        in.readFully(value);
        return value;
    }

    public static void command(DataOutputStream out, byte[] data) throws IOException {
        require(data.length > 0 && data.length <= MAX_COMMAND);
        out.writeInt(data.length);
        out.write(data);
    }

    public static Tx transaction(DataInputStream in) throws IOException {
        return new Tx(sequence(in, false), uuid(in), node(in), node(in), uuid(in), command(in));
    }

    public static void transaction(DataOutputStream out, Tx tx) throws IOException {
        require(tx.sequence() > 0);
        out.writeLong(tx.sequence());
        uuid(out, tx.id());
        node(out, tx.coordinator());
        node(out, tx.client());
        uuid(out, tx.request());
        command(out, tx.bytes());
    }

    public static Reference reference(Tx tx) { return new Reference(tx.sequence(), tx.id()); }

    public static Frame header(byte[] body) throws IOException {
        require(body.length <= MAX_BODY);
        var in = new DataInputStream(new ByteArrayInputStream(body));
        require(in.readInt() == MAGIC);
        return new Frame(new Header(in.readUnsignedByte(), in.readUnsignedByte(), uuid(in), node(in)), in);
    }

    public static byte[] encode(Header header, Writer writer) throws IOException {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeByte(header.algorithm());
            out.writeByte(header.kind());
            uuid(out, header.token());
            node(out, header.sender());
            writer.write(out);
        }
        require(bytes.size() <= MAX_BODY);
        return bytes.toByteArray();
    }

    public static Object readPayload(int kind, DataInputStream in) throws IOException {
        Object result;
        switch (kind) {
            case SUBMIT -> result = command(in);
            case PREPARE, COMMIT, ABORT -> result = transaction(in);
            case QUERY -> result = new Reference(sequence(in, false), uuid(in));
            case 0x82, 0x83, 0x84, 0x85 -> {
                var ref = new Reference(sequence(in, false), uuid(in));
                int status = in.readUnsignedByte();
                require(status <= (kind == 0x85 ? 2 : 1));
                result = new Reply(ref, status);
            }
            case 0x81 -> {
                int status = in.readUnsignedByte();
                long seq = sequence(in, true);
                require(status <= 4 && (status < 2 ? seq > 0 : status == 2 || seq == 0));
                result = new Result(status, seq, node(in));
            }
            case 0xff -> {
                int error = in.readUnsignedByte();
                require(error >= 1 && error <= 4);
                result = error;
            }
            default -> throw new IOException("Tipo 2PC inválido");
        }
        require(in.read() == -1);
        return result;
    }

    public static void writePayload(DataOutputStream out, Object value) throws IOException {
        if (value instanceof Tx tx) transaction(out, tx);
        else if (value instanceof byte[] bytes) command(out, bytes);
        else if (value instanceof Result result) {
            out.writeByte(result.status());
            out.writeLong(result.sequence());
            node(out, result.coordinator());
        } else if (value instanceof Reply reply) {
            writePayload(out, reply.reference());
            out.writeByte(reply.status());
        } else if (value instanceof Reference reference) {
            out.writeLong(reference.sequence());
            uuid(out, reference.id());
        } else if (value instanceof Integer code) out.writeByte(code);
        else throw new IOException("Tipo 2PC inválido");
    }

    public static Reply correlate(byte[] body, Header request, String peer, Reference reference) throws IOException {
        var frame = header(body);
        var response = frame.header();
        require(response.algorithm() == ALGORITHM && response.kind() == (request.kind() | 0x80)
                && response.sender().equals(peer) && response.token().equals(request.token()));
        if (!(readPayload(response.kind(), frame.input()) instanceof Reply reply)) throw new IOException("Respuesta 2PC");
        require(reply.reference().equals(reference));
        return reply;
    }

    public static byte[] roundTrip(byte[] body) throws IOException {
        var frame = header(body);
        require(frame.header().algorithm() == ALGORITHM);
        Object payload = readPayload(frame.header().kind(), frame.input());
        return encode(frame.header(), out -> writePayload(out, payload));
    }
}
