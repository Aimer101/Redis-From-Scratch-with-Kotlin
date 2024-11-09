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

    fun validateStreamId(key: String, id: String) : String? {
        val item = storage[key]


        if (item is RedisValue.StreamValue) {
            val requestedMilisecondNum = id.split("-")[0].toInt()
            var requestedCounterNum = id.split("-")[1].toInt()

            if(requestedCounterNum == "*") {
                return null
            }

            requestedCounterNum = requestedCounterNum.toInt()

            if(requestedCounterNum == 0 && requestedMilisecondNum == 0) {
                return Resp.simpleError("The ID specified in XADD must be greater than 0-0")
            }

            if (!item.entries.isEmpty()) {
                val currentCounterNum   = item.entries.last().id.split("-")[1].toInt()
                val currentMilisecondNum = item.entries.last().id.split("-")[0].toInt()

                if(requestedCounterNum <= currentCounterNum) {
                    return Resp.simpleError("The ID specified in XADD is equal or smaller than the target stream top item")
                }

                if(requestedMilisecondNum < currentMilisecondNum) {
                    return Resp.simpleError("The ID specified in XADD is equal or smaller than the target stream top item")
                }
            }
        }

        return null
    }


    fun handleXadd(key: String, id: String, fields : HashMap<String, String>) : String{

        synchronized(storage) {
            val item = storage[key]

            if (item is RedisValue.StringValue) {
                throw Exception("WRONGTYPE Operation against a key holding the wrong kind of value")
            }

            val currentMilisNum     = id.split("-")[0]
            val currentCounterNum   = id.split("-")[1]
            var entryId             = id

            if (currentCounterNum == "*") {
                // edge case is when timemilis is 0, then counter cannot be 0
                var minCounterNum = 1

                if(currentMilisNum != "0") {
                    minCounterNum = 0
                }

                if(storage[key] == null) {
                    entryId = "$currentMilisNum-$minCounterNum"
                } else {
                    if(storage[key].entries.isEmpty()) {
                        entryId = "$currentMilisNum-$minCounterNum"
                    } else {
                        val lastEntryId = storage[key].entries.last().id
                        val lastEntryIdCounterNum = lastEntryId.split("-")[1].toInt()
                        entryId = "$currentMilisNum-${lastEntryIdCounterNum + 1}"
                    }
                }
            }

            if (item is RedisValue.StreamValue) {
                item.entries.add(StreamEntry(entryId, fields))
            } else {
                storage[key] = RedisValue.StreamValue(mutableListOf(StreamEntry(entryId, fields)))
            }

            return id
        }
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