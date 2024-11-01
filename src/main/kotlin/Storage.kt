import java.util.concurrent.ConcurrentHashMap

object Storage {
    private val storage = ConcurrentHashMap<String, String>()

    fun set(key: String, value: String) {
        storage[key] = value
    }

    fun get(key: String): String? = storage[key]
}