fun ByteArray.toLittleEndianInt(): Int {
    require(this.size == 4) { "Byte array must be exactly 4 bytes for toLittleEndianInt()" }
    return (this[3].toInt() and 0xFF shl 24) or
           (this[2].toInt() and 0xFF shl 16) or
           (this[1].toInt() and 0xFF shl 8) or
           (this[0].toInt() and 0xFF)
}

fun ByteArray.toLittleEndianLong(): Long {
    require(this.size == 8) { "Byte array must be exactly 8 bytes for toLittleEndianLong()" }
    return (this[7].toLong() and 0xFF shl 56) or
           (this[6].toLong() and 0xFF shl 48) or
           (this[5].toLong() and 0xFF shl 40) or
           (this[4].toLong() and 0xFF shl 32) or
           (this[3].toLong() and 0xFF shl 24) or
           (this[2].toLong() and 0xFF shl 16) or
           (this[1].toLong() and 0xFF shl 8) or
           (this[0].toLong() and 0xFF)
}
