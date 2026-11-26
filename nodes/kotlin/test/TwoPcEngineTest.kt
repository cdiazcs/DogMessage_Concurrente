import dogmessenger.twopc.Engine
import dogmessenger.twopc.Wire
import java.net.InetSocketAddress
import java.nio.file.Files
import java.util.UUID

object TwoPcEngineTest {
    @JvmStatic
    fun main(args: Array<String>) {
        val root = Files.createTempDirectory("2pc kotlin ")
        val peers = mapOf("n1" to InetSocketAddress("127.0.0.1", 9101))
        val tx = Wire.Tx(1, UUID.randomUUID(), "n1", "cliente", UUID.randomUUID(), byteArrayOf(0, -1, -128))
        fun call(engine: Engine, kind: Int): Int {
            val head = Wire.Header(2, kind, UUID.randomUUID(), "n1")
            return Wire.correlate(engine.dispatch(Wire.encode(head) { Wire.transaction(it, tx) }),
                head, "n2", Wire.reference(tx)).status()
        }
        try {
            Engine("n2", "n1", peers, root, false).use { node ->
                check(call(node, Wire.COMMIT) == 0)
                check(call(node, Wire.PREPARE) == 1)
            }
            Engine("n2", "n1", peers, root, false).use { node ->
                check(call(node, Wire.ABORT) == 1)
            }
            Engine("n2", "n1", peers, root, false).use { node ->
                check(call(node, Wire.ABORT) == 1 && call(node, Wire.COMMIT) == 0)
            }
        } finally {
            Files.walk(root).use { paths -> paths.sorted(Comparator.reverseOrder()).forEach(Files::delete) }
        }
        println("PASS Kotlin 2PC PREPARED, ABORT durable y rechazo de COMMIT")
    }
}
