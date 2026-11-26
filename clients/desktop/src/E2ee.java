import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.*;
import java.security.spec.*;
import java.util.*;
import javax.crypto.*;
import javax.crypto.spec.*;

/** E1 usa exclusivamente proveedores criptográficos JCA/JCE. */
final class E2ee {
    static final SecureRandom RANDOM = new SecureRandom();
    private static final Base64.Encoder ENCODER = Base64.getEncoder();
    private static final Base64.Decoder DECODER = Base64.getDecoder();
    private static final HexFormat HEX = HexFormat.of();
    private static final String SIGNATURE = "SHA256withECDSAinP1363Format";

    record Keys(String id, String exchange, String signing) {}
    record Device(Keys publicKeys, PrivateKey dh, PrivateKey sign) {}
    record Context(String conversation, String message, String device, String epoch, String purpose) {
        Context(String conversation, String message, String device, String epoch) {
            this(conversation, message, device, epoch, "text");
        }
        String aad() {
            for (String id : List.of(conversation, message, device)) {
                if (!id.matches("[0-9a-f]{32}")) throw new IllegalArgumentException("Contexto E2EE inválido");
            }
            if (!epoch.matches("[0-9a-f]{64}") || !Set.of("text", "file", "grant").contains(purpose)) throw new IllegalArgumentException("Época E2EE inválida");
            return String.join("\n", "dog.e2ee.1", conversation, message, device, epoch, purpose);
        }
    }

    private static byte[] utf8(String text) { return text.getBytes(StandardCharsets.UTF_8); }
    static byte[] random(int size) { byte[] data = new byte[size]; RANDOM.nextBytes(data); return data; }
    static String b64(byte[] value) { return ENCODER.encodeToString(value); }
    static byte[] raw(String value) { return DECODER.decode(value); }
    static byte[] sha(byte[] bytes) throws GeneralSecurityException {
        return MessageDigest.getInstance("SHA-256").digest(bytes);
    }

    private static PublicKey publicKey(String spki) throws GeneralSecurityException {
        return KeyFactory.getInstance("EC").generatePublic(new X509EncodedKeySpec(raw(spki)));
    }

    static Device generate() throws GeneralSecurityException {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        KeyPair dh = generator.generateKeyPair(), sign = generator.generateKeyPair();
        String exchange = b64(dh.getPublic().getEncoded()), signing = b64(sign.getPublic().getEncoded());
        return new Device(new Keys(fingerprint(exchange, signing).substring(0, 32), exchange, signing),
                          dh.getPrivate(), sign.getPrivate());
    }

    static String fingerprint(String exchange, String signing) throws GeneralSecurityException {
        byte[] first = raw(exchange), second = raw(signing);
        return HEX.formatHex(sha(ByteBuffer.allocate(first.length + second.length).put(first).put(second).array()));
    }

    static String proof(Device device, String request, String token) throws GeneralSecurityException {
        String session = HEX.formatHex(sha(HEX.parseHex(token)));
        return b64(signature(device.sign(), String.join("\n", "dog.device.1", request, session, device.publicKeys().id())));
    }

    private static byte[] signature(PrivateKey key, String value) throws GeneralSecurityException {
        Signature signature = Signature.getInstance(SIGNATURE);
        signature.initSign(key, RANDOM);
        signature.update(utf8(value));
        return signature.sign();
    }

    static byte[] cipher(int mode, byte[] key, byte[] nonce, String aad, byte[] bytes) throws GeneralSecurityException {
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(mode, new SecretKeySpec(key, "AES"), new GCMParameterSpec(128, nonce));
        cipher.updateAAD(utf8(aad));
        return cipher.doFinal(bytes);
    }

    static byte[] hkdf(byte[] shared, byte[] salt, byte[] info) throws GeneralSecurityException {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(salt, "HmacSHA256"));
        byte[] prk = mac.doFinal(shared);
        try {
            mac.init(new SecretKeySpec(prk, "HmacSHA256"));
            mac.update(info);
            return mac.doFinal(new byte[]{1});
        } finally { Arrays.fill(prk, (byte)0); }
    }

    private static byte[] wrapping(PrivateKey privateKey, String exchange, String aad, String recipient)
            throws GeneralSecurityException {
        KeyAgreement agreement = KeyAgreement.getInstance("ECDH");
        agreement.init(privateKey);
        agreement.doPhase(publicKey(exchange), true);
        byte[] shared = agreement.generateSecret();
        try { return hkdf(shared, sha(utf8(aad)), utf8("dog.wrap.1\n" + recipient)); }
        finally { Arrays.fill(shared, (byte)0); }
    }

    static String seal(String text, Context context, Device sender, List<Keys> recipients)
            throws GeneralSecurityException {
        byte[] plain = utf8(text);
        if (text.isBlank() || plain.length > 4096 || recipients.isEmpty() || recipients.size() > 16) {
            throw new IllegalArgumentException("Texto o destinatarios fuera de límite");
        }
        String aad = context.aad();
        byte[] key = random(32), nonce = random(12);
        try {
            byte[] ciphertext = cipher(Cipher.ENCRYPT_MODE, key, nonce, aad, plain);
            List<String> wraps = new ArrayList<>();
            for (Keys recipient : recipients.stream().sorted(Comparator.comparing(Keys::id)).toList()) {
                byte[] iv = random(12), kek = wrapping(sender.dh(), recipient.exchange(), aad, recipient.id());
                byte[] wrapped;
                try { wrapped = cipher(Cipher.ENCRYPT_MODE, kek, iv, aad + "\n" + recipient.id(), key); }
                finally { Arrays.fill(kek, (byte)0); }
                wraps.add(String.join(":", recipient.id(), b64(iv), b64(wrapped)));
            }
            String body = String.join(".", "E1", b64(nonce), b64(ciphertext), String.join(";", wraps));
            return body + "." + b64(signature(sender.sign(), aad + "\n" + body));
        } finally { Arrays.fill(key, (byte)0); }
    }

    private static String[] validate(String box, Context context, Keys sender) throws GeneralSecurityException {
        if (box.length() > 8192 || !fingerprint(sender.exchange(), sender.signing()).startsWith(context.device())) {
            throw new GeneralSecurityException("Identidad del emisor alterada");
        }
        String[] fields = box.split("\\.", -1);
        if (fields.length != 5 || !fields[0].equals("E1")) throw new GeneralSecurityException("Sobre E2EE inválido");
        Signature verifier = Signature.getInstance(SIGNATURE);
        verifier.initVerify(publicKey(sender.signing()));
        verifier.update(utf8(context.aad() + "\n" + box.substring(0, box.lastIndexOf('.'))));
        if (!verifier.verify(raw(fields[4]))) throw new GeneralSecurityException("Firma del mensaje inválida");
        return fields;
    }

    private static byte[] unwrap(String[] fields, Context context, Keys sender, Device recipient)
            throws GeneralSecurityException {
        String id = recipient.publicKeys().id();
        String aad = context.aad();
        for (String row : fields[3].split(";")) {
            String[] entry = row.split(":", -1);
            if (entry.length != 3 || !entry[0].equals(id)) continue;
            byte[] kek = wrapping(recipient.dh(), sender.exchange(), aad, id);
            try { return cipher(Cipher.DECRYPT_MODE, kek, raw(entry[1]), aad + "\n" + id, raw(entry[2])); }
            finally { Arrays.fill(kek, (byte)0); }
        }
        throw new GeneralSecurityException("Este dispositivo no recibió la clave");
    }

    static String exportKey(String box, Context context, Keys sender, Device recipient)
            throws GeneralSecurityException {
        String[] fields = validate(box, context, sender);
        byte[] key = unwrap(fields, context, sender, recipient);
        try {
            cipher(Cipher.DECRYPT_MODE, key, raw(fields[1]), context.aad(), raw(fields[2]));
            return b64(key);
        } finally { Arrays.fill(key, (byte)0); }
    }

    static String open(String box, Context context, Keys sender, Device recipient) throws GeneralSecurityException {
        return open(box, context, sender, recipient, null);
    }

    static String open(String box, Context context, Keys sender, Device recipient, String historyKey)
            throws GeneralSecurityException {
        String[] fields = validate(box, context, sender);
        byte[] key = historyKey == null ? unwrap(fields, context, sender, recipient) : raw(historyKey);
        try {
            if (key.length != 32) throw new GeneralSecurityException("Clave fuera de contrato");
            return new String(cipher(Cipher.DECRYPT_MODE, key, raw(fields[1]), context.aad(), raw(fields[2])), StandardCharsets.UTF_8);
        } finally { Arrays.fill(key, (byte)0); }
    }

    static String signLink(Device device, String action, String request, String token, String... fields)
            throws GeneralSecurityException {
        String session = HEX.formatHex(sha(HEX.parseHex(token)));
        String statement = String.join("\n", "dog.link." + action + ".1", request, session, device.publicKeys().id(),
                                       String.join("\n", fields));
        return b64(signature(device.sign(), statement));
    }

    private static byte[] storageKey(char[] passphrase, byte[] salt) throws GeneralSecurityException {
        if (passphrase.length < 12) throw new GeneralSecurityException("Usa al menos 12 caracteres para la clave local");
        var spec = new PBEKeySpec(passphrase, salt, 600_000, 256);
        try { return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded(); }
        finally { spec.clearPassword(); }
    }

    static Device store(Path target, char[] passphrase, String account) throws Exception {
        Path guard = target.resolveSibling("identity.lock");
        try (var channel = FileChannel.open(guard, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             var lock = channel.tryLock()) {
            if (lock == null) throw new java.io.IOException("La identidad local está en uso");
            return storeLocked(target, passphrase, account);
        }
    }

    private static Device storeLocked(Path target, char[] passphrase, String account) throws Exception {
        String aad = "dog.desktop.device.1\n" + account;
        if (Files.exists(target)) {
            if (Files.size(target) > 4096) throw new GeneralSecurityException("Identidad local inválida");
            ByteBuffer file = ByteBuffer.wrap(Files.readAllBytes(target));
            byte[] salt = new byte[16], nonce = new byte[12];
            if (file.remaining() < 44) throw new GeneralSecurityException("Identidad local inválida");
            file.get(salt).get(nonce);
            byte[] ciphertext = new byte[file.remaining()];
            file.get(ciphertext);
            byte[] key = storageKey(passphrase, salt);
            try {
                var value = Json.map(Json.decode(new String(cipher(Cipher.DECRYPT_MODE, key, nonce, aad, ciphertext), StandardCharsets.UTF_8)));
                String exchange = (String)value.get("exchange"), signing = (String)value.get("signing");
                KeyFactory factory = KeyFactory.getInstance("EC");
                return new Device(new Keys(fingerprint(exchange, signing).substring(0, 32), exchange, signing),
                    factory.generatePrivate(new PKCS8EncodedKeySpec(raw((String)value.get("dh")))),
                    factory.generatePrivate(new PKCS8EncodedKeySpec(raw((String)value.get("sign")))));
            } finally { Arrays.fill(key, (byte)0); }
        }
        Device device = generate();
        byte[] salt = random(16), nonce = random(12), key = storageKey(passphrase, salt);
        byte[] ciphertext;
        try {
            var payload = Map.of("exchange", device.publicKeys().exchange(), "signing", device.publicKeys().signing(),
                                 "dh", b64(device.dh().getEncoded()), "sign", b64(device.sign().getEncoded()));
            ciphertext = cipher(Cipher.ENCRYPT_MODE, key, nonce, aad, utf8(Json.encode(payload)));
        } finally { Arrays.fill(key, (byte)0); }
        Path temp = Files.createTempFile(target.getParent(), "identity-", ".tmp");
        try {
            Files.write(temp, ByteBuffer.allocate(28 + ciphertext.length).put(salt).put(nonce).put(ciphertext).array());
            Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE);
        } finally { Files.deleteIfExists(temp); }
        return device;
    }
}
