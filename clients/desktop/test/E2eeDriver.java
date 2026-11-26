import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/** Herramienta de ensayo. Las claves privadas solo viven en este proceso. */
public final class E2eeDriver {
    private static final Map<String, E2ee.Device> devices = new HashMap<>();

    private static String text(Map<String, Object> input, String name) { return (String)input.get(name); }
    private static E2ee.Keys keys(Map<String, Object> input) {
        return new E2ee.Keys(text(input, "id"), text(input, "exchange"), text(input, "signing"));
    }

    private static Object execute(Map<String, Object> input) throws Exception {
        String name = text(input, "name"), op = text(input, "op");
        if ("generate".equals(op)) {
            var device = E2ee.generate();
            devices.put(name, device);
            var keys = device.publicKeys();
            return Map.of("id", keys.id(), "exchange", keys.exchange(), "signing", keys.signing());
        }
        if ("file-seal".equals(op) || "file-open".equals(op)) {
            var path = java.nio.file.Files.createTempFile("dog-e2ee-test-", ".bin");
            try {
                java.nio.file.Files.write(path, E2ee.raw(text(input, "bytes")));
                String conversation = text(input, "conversation"), message = text(input, "message");
                if ("file-seal".equals(op)) {
                    var sealed = E2eeFile.encrypt(path, conversation, message);
                    try {
                        return Map.of("bytes", E2ee.b64(java.nio.file.Files.readAllBytes(sealed.path())),
                            "key", sealed.key(), "sha256", sealed.sha256());
                    } finally { java.nio.file.Files.deleteIfExists(sealed.path()); }
                }
                var output = new java.io.ByteArrayOutputStream();
                E2eeFile.decrypt(path, output, text(input, "key"), (Long)input.get("size"), conversation, message);
                return E2ee.b64(output.toByteArray());
            } finally { java.nio.file.Files.deleteIfExists(path); }
        }
        var device = devices.get(name);
        if ("proof".equals(op)) return E2ee.proof(device, text(input, "request"), text(input, "token"));
        if ("link-proof".equals(op)) {
            String[] fields = ((java.util.List<?>)input.get("fields")).toArray(String[]::new);
            return E2ee.signLink(device, text(input, "purpose"), text(input, "request"), text(input, "token"), fields);
        }
        var meta = Json.map(input.get("meta"));
        var context = new E2ee.Context(text(meta, "conversation"), text(meta, "message"), text(meta, "device"),
                                      text(meta, "epoch"), (String)meta.getOrDefault("purpose", "text"));
        if ("seal".equals(op)) {
            return E2ee.seal(text(input, "text"), context, device, Json.rows(input.get("devices")).stream()
                            .map(E2eeDriver::keys).toList());
        }
        if ("open".equals(op)) return E2ee.open(text(input, "box"), context, keys(Json.map(input.get("sender"))),
            device, text(input, "historyKey"));
        if ("export-key".equals(op)) return E2ee.exportKey(text(input, "box"), context, keys(Json.map(input.get("sender"))), device);
        throw new IllegalArgumentException("Unknown operation");
    }

    public static void main(String[] args) throws Exception {
        try (var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            for (String line; (line = input.readLine()) != null;) {
                try {
                    Object result = execute(Json.map(Json.decode(line)));
                    System.out.println(Json.encode(Map.of("result", result)));
                } catch (Exception error) {
                    System.out.println(Json.encode(Map.of("error", error.getClass().getSimpleName())));
                }
            }
        }
    }
}
