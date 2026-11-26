import dogmessenger.twopc.Wire

/** Kotlin y Java usan el mismo codec JVM para el algoritmo DM2 0x02. */
internal object TwoPcWire {
    fun roundTrip(body: ByteArray): ByteArray = Wire.roundTrip(body)
}
