import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.ConcurrentHashMap

object Storage {
    private val storage = ConcurrentHashMap<String, String>()

    fun set(key: String, value: String, expire: Int = 0) {
        storage[key] = value

        if (expire > 0) {
            Timer().schedule(object : TimerTask() {
                override fun run() {
                    storage.remove(key)
                }
             }, expire.toLong())
        }
    }

    fun get(key: String): String? = storage[key]
}