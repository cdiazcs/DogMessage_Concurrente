import dogmessenger.chat.Envelope
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.HexFormat

object ChatEnvelopeTest {
    @JvmStatic
    fun main(args: Array<String>) {
        val hex = HexFormat.of()
        val rows = Files.readAllLines(Path.of("protocol/fixtures/chat.tsv"))
        for (row in rows) {
            val (label, expected, data) = row.split('\t')
            val raw = hex.parseHex(data)
            try {
                val op = Envelope.decode(raw, ByteArray(32))
                check(expected == "1" && Envelope.encode(op, ByteArray(32)).contentEquals(raw)) { label }
            } catch (error: IOException) {
                check(expected == "0") { label }
            }
        }
        val (salt, password, digest) = Files.readString(Path.of("protocol/fixtures/chat_pbkdf2.tsv")).trim().split('\t')
        check(hex.formatHex(Envelope.derive(hex.parseHex(password).decodeToString().toCharArray(),
            hex.parseHex(salt))) == digest)
        println("PASS Kotlin CHAT1: ${rows.size} fixtures + PBKDF2")
    }
}
