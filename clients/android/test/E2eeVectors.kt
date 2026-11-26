// Valida el proveedor Kotlin/JVM de escritorio. No constituye una prueba Android.
import java.io.File
import java.util.HexFormat
import javax.crypto.Cipher

fun main() {
    val hex = HexFormat.of()
    val fixtures = File("protocol/fixtures/e2ee.tsv").readLines().filterNot { it.startsWith("#") }
    for (fixture in fixtures) {
        val row = fixture.split('\t')
        val key = hex.parseHex(row[1])
        val param = hex.parseHex(row[2])
        val data = hex.parseHex(row[3])
        val result = if (row[0] == "gcm") {
            E2ee.cipher(Cipher.ENCRYPT_MODE, key, param, "", data)
        } else {
            E2ee.hkdf(key, param, data)
        }
        check(result.contentEquals(hex.parseHex(row[4])))
    }
    println("PASS Kotlin/JVM E2EE: ${fixtures.size} vectores")
    mobileCompatibility()
}

private fun mobileCompatibility() {
    val codec = com.pc4.dogmessenger.MobileE1(
        { java.util.Base64.getEncoder().encodeToString(it) },
        { java.util.Base64.getDecoder().decode(it) }
    )
    val javaDevice = E2ee.generate()
    val native = codec.generate()
    val pub = javaDevice.publicKeys()
    val mobileSender = com.pc4.dogmessenger.MobileE1.Keys(pub.id(), pub.exchange(), pub.signing())
    val desktopRecipient = E2ee.Keys(native.keys.id, native.keys.exchange, native.keys.signing)
    val conv = "1".repeat(32)
    val msg = "2".repeat(32)
    val epoch = "a".repeat(64)
    val first = E2ee.seal("Java a Kotlin 🐶", E2ee.Context(conv, msg, pub.id(), epoch),
        javaDevice, listOf(desktopRecipient))
    check(codec.open(first, com.pc4.dogmessenger.MobileE1.Meta(conv, msg, pub.id(), epoch),
        mobileSender, native) == "Java a Kotlin 🐶")
    val second = codec.seal("Kotlin a Java 🐶",
        com.pc4.dogmessenger.MobileE1.Meta(conv, msg, native.keys.id, epoch), native, listOf(mobileSender))
    check(E2ee.open(second, E2ee.Context(conv, msg, native.keys.id, epoch),
        desktopRecipient, javaDevice) == "Kotlin a Java 🐶")
    val broken = second.substringBeforeLast('.') + "." + java.util.Base64.getEncoder().encodeToString(ByteArray(64))
    check(runCatching { E2ee.open(broken, E2ee.Context(conv, msg, native.keys.id, epoch), desktopRecipient, javaDevice) }.isFailure)
    println("PASS codec Kotlin E1 ↔ Java y rechazo de firma")
    val input = java.nio.file.Files.createTempFile("dog-kotlin-dgf-", ".bin")
    try {
        val clear = ByteArray(17_000) { (it % 251).toByte() }
        java.nio.file.Files.write(input, clear)
        val sealed = E2eeFile.encrypt(input, conv, msg)
        try {
            val opened = java.io.ByteArrayOutputStream()
            java.nio.file.Files.newInputStream(sealed.path()).use {
                codec.openFile(it, opened, java.nio.file.Files.size(sealed.path()), clear.size.toLong(),
                    sealed.key(), conv, msg)
            }
            check(opened.toByteArray().contentEquals(clear))
        } finally { java.nio.file.Files.deleteIfExists(sealed.path()) }
        val out = java.io.ByteArrayOutputStream()
        val key = codec.sealFile(clear.inputStream(), out, clear.size.toLong(), conv, msg) { false }
        java.nio.file.Files.write(input, out.toByteArray())
        val javaPlain = java.io.ByteArrayOutputStream()
        E2eeFile.decrypt(input, javaPlain, key, clear.size.toLong(), conv, msg)
        check(javaPlain.toByteArray().contentEquals(clear))
        println("PASS DGF1 Kotlin ↔ Java, tres bloques")
    } finally { java.nio.file.Files.deleteIfExists(input) }
}
