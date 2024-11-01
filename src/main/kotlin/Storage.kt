import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.ConcurrentHashMap


object Storage {
    private val storage = ConcurrentHashMap<String, String>()

    fun set(key: String, value: String, expire: Int? = null) {
        storage[key] = value

        if (expire != null) {
            Timer().schedule(object : TimerTask() {
                override fun run() {
                    storage.remove(key)
                }
             }, expire.toLong())
        }
    }

    fun get(key: String): String? = storage[key]

    fun getAllMatchingKeys(pattern: String): List<String> {
        val dbFilePath = helpers.getDbFilePath()
        val matchingKeys = mutableListOf<String>()
        val dbHeaderOpcode = 0xFE.toByte()

        val regexPattern = pattern.replace("*", ".*").toRegex()

        try {

        } catch (e: IOException) {
        println("Error while reading RDB file: $e")
        }

    }
}