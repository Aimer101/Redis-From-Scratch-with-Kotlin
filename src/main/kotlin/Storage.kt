import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.ConcurrentHashMap

sealed class RedisValue {
    data class StringValue(val value: String) : RedisValue()
    data class StreamValue(val entries: MutableList<StreamEntry>) : RedisValue()
}

data class StreamEntry(
    val id : String,
    val fields: HashMap<String, String>
)

object Storage {
    private val storage = ConcurrentHashMap<String, RedisValue>()

    fun set(key: String, value: String, expire: Int? = null) {
        storage[key] = RedisValue.StringValue(value)

        if (expire != null) {
            Timer().schedule(object : TimerTask() {
                override fun run() {
                    storage.remove(key)
                }
             }, expire.toLong())
        }

        println("${DBConfig.getRoleInfo()} Stored key: $key value: $value with expiry of $expire")

    }

    fun get(key: String): String? {
        val item = storage[key]

        if(item is RedisValue.StringValue) {
            return item.value
        }

        return null
    }

    fun handleXadd(key: String, id: String, fields : HashMap<String, String>) : String{
        val item = storage[key]

        if (item is RedisValue.StringValue) {
            throw Exception("WRONGTYPE Operation against a key holding the wrong kind of value")
        } else if (item is RedisValue.StreamValue) {
            item.entries.add(StreamEntry(id, fields))
        } else {
            storage[key] = RedisValue.StreamValue(mutableListOf(StreamEntry(id, fields)))
        }

        return id
    }

    fun getType(key: String): String {
        val item = storage[key]

        if (item is RedisValue.StringValue) {
            return Resp.STRING
        } else if (item is RedisValue.StreamValue) {
            return Resp.STREAM
        }

        return Resp.NONE
    }

}