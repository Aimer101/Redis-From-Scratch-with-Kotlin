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

        println("${DBConfig.getRoleInfo()} Stored key: $key value: $value with expiry of $expire")

    }

    fun get(key: String): String? = storage[key]

}