import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.swing.ImageIcon;

final class MediaTransfer {
    private static final long LIMIT = 50L * 1024 * 1024;
    private final NetworkClient network;
    private final String session;
    private final String minimum;

    MediaTransfer(NetworkClient network, String session, String minimum) {
        this.network = network;
        this.session = session;
        this.minimum = minimum;
    }

    private Map<String, Object> request(NetworkClient.Request req) throws IOException {
        var response = network.call(req, session, minimum);
        if (!Boolean.TRUE.equals(response.get("ok"))) throw new IOException(String.valueOf(response.get("error")));
        return Json.map(response.get("result"));
    }

    private Map<String, Object> request(String operation, Map<String, Object> data) throws IOException {
        return request(new NetworkClient.Request(operation, data));
    }

    Map<String, Object> directory(String conversation) throws IOException {
        return request("e2ee-directory", Map.of("conversation", conversation));
    }

    static class Upload {
        final Path original;
        Path path;
        final String conversation;
        final AtomicBoolean cancel = new AtomicBoolean();
        private NetworkClient.Request init, complete;
        volatile boolean completing;

        Upload(Path path, String conversation) {
            this.original = this.path = path;
            this.conversation = conversation;
        }
    }

    void prepareEncrypted(Upload state, E2ee.Device device, String epoch,
                          java.util.List<E2ee.Keys> recipients) throws Exception {
        if (state.init != null) return;
        String message = java.util.UUID.randomUUID().toString().replace("-", "");
        String name = state.original.getFileName().toString();
        if (!validName(name)) throw new IOException("Nombre de archivo inválido");
        var sealed = E2eeFile.encrypt(state.original, state.conversation, message);
        boolean prepared = false;
        try {
            String image = Files.probeContentType(state.original);
            String type = "image/png".equals(image) || "image/jpeg".equals(image) ? image : "application/octet-stream";
            String id = device.publicKeys().id();
            var ticket = Map.of("filename", name, "size", sealed.size(), "type", type,
                                "sha256", sealed.sha256(), "key", sealed.key());
            String box = E2ee.seal(Json.encode(ticket), new E2ee.Context(state.conversation, message, id, epoch, "file"),
                                   device, recipients);
            state.init = new NetworkClient.Request("upload-encrypted-init", Map.of(
                "conversation", state.conversation, "size", Files.size(sealed.path()), "sha256", sealed.sha256(),
                "message", message, "device", id, "epoch", epoch, "envelope", box));
            state.complete = new NetworkClient.Request("upload-complete", Map.of("transfer", state.init.id()), message);
            state.path = sealed.path();
            prepared = true;
        } finally {
            if (!prepared) Files.deleteIfExists(sealed.path());
        }
    }

    static boolean validName(String name) {
        return name != null && !name.isBlank() && !name.equals(".") && !name.equals("..")
            && name.getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= 240
            && name.chars().noneMatch(c -> c == '/' || c == '\\' || c == ':' || c < 32 || c >= 127 && c <= 159
                                        || c >= 0x202a && c <= 0x202e || c >= 0x2066 && c <= 0x2069);
    }

    Map<String, Object> upload(Upload state, BiConsumer<Integer, String> progress) throws Exception {
        long size = Files.size(state.path);
        if (size < 1 || size > LIMIT) throw new IOException("El archivo debe tener entre 1 byte y 50 MiB");
        if (state.init == null) {
            String sha = sha256(state.path);
            state.init = new NetworkClient.Request("upload-init", Map.of("conversation", state.conversation,
                "filename", state.path.getFileName().toString(), "size", size, "sha256", sha,
                "content_type", "application/octet-stream"));
            state.complete = new NetworkClient.Request("upload-complete", Map.of("transfer", state.init.id()));
        }
        Map<String, Object> reference = Map.of("transfer", state.init.id());
        request(state.init);
        var status = request("upload-status", reference);
        if (!state.completing) {
            long offset = number(status, "received");
            int blockSize = blockSize(status);
            try (var input = new RandomAccessFile(state.path.toFile(), "r")) {
                input.seek(offset);
                while (offset < size && !state.cancel.get()) {
                    int n = (int)Math.min(blockSize, size - offset);
                    byte[] chunk = new byte[n];
                    input.readFully(chunk);
                    var reply = request("upload-chunk", Map.of("transfer", state.init.id(),
                        "offset", offset, "block", Base64.getEncoder().encodeToString(chunk)));
                    if (number(reply, "received") != offset + n) throw new IOException("Bloque no confirmado");
                    offset += n;
                    progress.accept((int)(100 * offset / size), "Enviando bloques");
                }
            }
            if (state.cancel.get()) return request("upload-cancel", reference);
            state.completing = true;
        }
        Map<String, Object> result = Map.of("status", "PENDING");
        for (int attempt = 0; attempt < 25; attempt++) {
            result = request(state.complete);
            String phase = String.valueOf(result.get("status"));
            progress.accept(100, phase);
            if (!phase.equals("REPLICATING") && !phase.equals("PENDING")) return result;
            Thread.sleep(700);
        }
        return result;
    }

    void download(Map<String, Object> attachment, Path target, BiConsumer<Integer, String> progress)
            throws Exception {
        String id = (String)attachment.get("id");
        long size = number(attachment, "size");
        if (size < 1 || size > LIMIT) throw new IOException("Tamaño inválido");
        Map<String, Object> ready = Map.of();
        for (int attempt = 0; attempt < 24; attempt++) {
            ready = request("download-init", Map.of("attachment", id));
            if ("READY".equals(ready.get("status"))) break;
            progress.accept(0, "Verificando una réplica");
            Thread.sleep(500);
        }
        if (!"READY".equals(ready.get("status"))) throw new IOException("Contenido temporalmente inaccesible");
        int blockSize = blockSize(ready);
        Path part = Files.createTempFile(target.toAbsolutePath().getParent(), "dog-attachment-", ".part");
        var hash = MessageDigest.getInstance("SHA-256");
        try {
            try (OutputStream output = Files.newOutputStream(part)) {
                for (long offset = 0; offset < size;) {
                    var block = request("download-chunk", Map.of("attachment", id, "offset", offset));
                    byte[] bytes = Base64.getDecoder().decode((String)block.get("block"));
                    if (number(block, "offset") != offset || bytes.length != Math.min(blockSize, size - offset)) {
                        throw new IOException("Bloque incompleto");
                    }
                    output.write(bytes);
                    hash.update(bytes);
                    offset += bytes.length;
                    progress.accept((int)(100 * offset / size), "Descargando bloques");
                }
            }
            if (!HexFormat.of().formatHex(hash.digest()).equals(attachment.get("sha256"))) {
                throw new IOException("Integridad SHA-256 incorrecta");
            }
            try {
                Files.move(part, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException ignored) {
                Files.move(part, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(part);
        }
    }

    void downloadEncrypted(Map<String, Object> attachment, Map<String, Object> ticket, String conversation,
                           Path target, BiConsumer<Integer, String> progress) throws Exception {
        Path sealed = Files.createTempFile("dog-dgf-download-", ".bin");
        try {
            download(attachment, sealed, progress);
            Path plain = Files.createTempFile(target.toAbsolutePath().getParent(), "dog-attachment-", ".part");
            try {
                try (var output = Files.newOutputStream(plain)) {
                    E2eeFile.decrypt(sealed, output, (String)ticket.get("key"), number(ticket, "size"),
                                    conversation, (String)attachment.get("id"));
                }
                try {
                    Files.move(plain, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (AtomicMoveNotSupportedException ignored) {
                    Files.move(plain, target, StandardCopyOption.REPLACE_EXISTING);
                }
            } finally { Files.deleteIfExists(plain); }
        } finally { Files.deleteIfExists(sealed); }
    }

    ImageIcon preview(Map<String, Object> attachment, Map<String, Object> ticket, String conversation,
                      BiConsumer<Integer, String> progress) throws Exception {
        var image = ticket == null ? attachment : ticket;
        String type = (String)image.get(ticket == null ? "content_type" : "type");
        if (!java.util.Set.of("image/png", "image/jpeg").contains(type)
                || number(image, "size") > 2_097_152) throw new IOException("Vista previa no disponible");
        Path file = Files.createTempFile("dog-preview-", ".image");
        try {
            download(attachment, file, progress);
            if (ticket == null) {
                try (var input = ImageIO.createImageInputStream(file.toFile())) { return preview(input, type); }
            }
            var plain = new java.io.ByteArrayOutputStream();
            E2eeFile.decrypt(file, plain, (String)ticket.get("key"), number(ticket, "size"),
                             conversation, (String)attachment.get("id"));
            try (var input = ImageIO.createImageInputStream(new java.io.ByteArrayInputStream(plain.toByteArray()))) {
                return preview(input, type);
            }
        } finally { Files.deleteIfExists(file); }
    }

    private ImageIcon preview(javax.imageio.stream.ImageInputStream input, String type) throws IOException {
        var readers = ImageIO.getImageReaders(input);
        if (!readers.hasNext()) throw new IOException("Formato de imagen inválido");
        ImageReader reader = readers.next();
        try {
            String format = "image/png".equals(type) ? "png" : "jpeg";
            if (!format.equalsIgnoreCase(reader.getFormatName())) throw new IOException("Tipo de imagen inesperado");
            reader.setInput(input, true, true);
            int width = reader.getWidth(0), height = reader.getHeight(0);
            if (width < 1 || height < 1 || (long)width * height > 8_000_000) {
                throw new IOException("Dimensiones fuera del límite de vista previa");
            }
            var image = reader.read(0);
            int scale = Math.max(1, Math.max(width / 480, height / 320));
            return new ImageIcon(image.getScaledInstance(width / scale, height / scale, java.awt.Image.SCALE_SMOOTH));
        } finally { reader.dispose(); }
    }

    private static long number(Map<String, Object> data, String key) { return ((Number)data.get(key)).longValue(); }

    private static int blockSize(Map<String, Object> data) throws IOException {
        long size = number(data, "block_size");
        if (size != 8192) throw new IOException("Tamaño de bloque fuera del contrato");
        return (int)size;
    }

    private static String sha256(Path path) throws Exception {
        MessageDigest hash = MessageDigest.getInstance("SHA-256");
        try (InputStream input = Files.newInputStream(path)) {
            byte[] bytes = new byte[64 * 1024];
            for (int count; (count = input.read(bytes)) != -1;) hash.update(bytes, 0, count);
        }
        return HexFormat.of().formatHex(hash.digest());
    }
}
