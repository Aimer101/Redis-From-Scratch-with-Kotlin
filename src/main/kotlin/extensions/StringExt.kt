fun String.decodeHexString(): ByteArray {
    check(length % 2 == 0) { "Must have an even length" }

    return ByteArray(length / 2) { index ->
        val startIndex = index * 2
        val hex = substring(startIndex, startIndex + 2)
        hex.toInt(16).toByte()
    }
}