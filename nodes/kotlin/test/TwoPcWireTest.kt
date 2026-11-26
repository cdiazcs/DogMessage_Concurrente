import java.io.IOException
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.util.HexFormat

object TwoPcWireTest {
    @JvmStatic
    fun main(args: Array<String>) {
        val lines = Files.readAllLines(Path.of("protocol/fixtures/two_pc.tsv"))
        for (line in lines) {
            val (label, valid, hex) = line.split('\t')
            val frame = HexFormat.of().parseHex(hex)
            val body = frame.copyOfRange(4, frame.size)
            check(ByteBuffer.wrap(frame).int == body.size) { label }
            try {
                check(TwoPcWire.roundTrip(body).contentEquals(body) && valid == "1") { label }
            } catch (error: IOException) {
                check(valid == "0") { "$label: $error" }
            }
        }
        println("PASS Kotlin fixtures 2PC: ${lines.size}")
    }
}
