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

    fun getStreamEntries(key: String): MutableList<StreamEntry>? {
        val item = storage[key]

        if(item is RedisValue.StreamValue) {
            return item.entries
        }

        return null
    }

    fun validateStreamId(key: String, id: String) : String? {
        val item = storage[key]

        if (item is RedisValue.StreamValue) {
            val requestedMilisecondNum = id.split("-")[0].toInt()
            var rawRequestedCounterNum = id.split("-")[1]

            if(rawRequestedCounterNum == "*") {
                return null
            }

            val requestedCounterNum = rawRequestedCounterNum.toInt()

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
                    val lastEntryId = (storage[key] as RedisValue.StreamValue).entries.last().id
                    val lastEntryIdMilisNum = lastEntryId.split("-")[0].toInt()
                    val lastEntryIdCounterNum = lastEntryId.split("-")[1].toInt()

                    if(lastEntryIdMilisNum == currentMilisNum.toInt()) {
                        entryId = "$currentMilisNum-${lastEntryIdCounterNum + 1}"
                    } else {
                        entryId = "$currentMilisNum-0"
                    }
                }

            }

            if (item is RedisValue.StreamValue) {
                item.entries.add(StreamEntry(entryId, fields))
            } else {
                storage[key] = RedisValue.StreamValue(mutableListOf(StreamEntry(entryId, fields)))
            }

            return entryId
        }
    }

    fun handleXRange(key: String, start: String, end: String) : ArrayList<StreamEntry> {
        var startId = start
        var endId   = end

        val result = ArrayList<StreamEntry>()

        if(startId.split("-").size != 2) {
            startId = "${startId}-0"
        }

        if(endId.split("-").size != 2) {
            endId = "${endId}-0"
        }

        synchronized(storage) {
            val item = storage[key]

            var isInRange = if (startId == "-") true else false

            for(entry in (item as RedisValue.StreamValue).entries) {
                if(entry.id == startId) {
                    isInRange = true
                    result.add(entry)
                    continue
                }

                if(isInRange) {
                    result.add(entry)
                }

                if(endId == "+") {
                    continue
                }

                if(entry.id == endId) {
                    break
                }
            }
        }

        return result
    }

    fun handleXRead(keys: List<String>, entryIds: List<String>) : ArrayList<ArrayList<StreamEntry>> {
        val result = ArrayList<ArrayList<StreamEntry>>()

        synchronized(storage) {

            for(i in 0 until keys.size) {
                val key = keys[i]
                val entryId = entryIds[i]
                val item = storage[key]
                val tempArr = ArrayList<StreamEntry>()

                if(entryId == "$") {
                    for(entry in (item as RedisValue.StreamValue).entries) {
                        tempArr.add(entry)
                    }
                } else {
                    val requestedTimeMilis = entryId.split("-")[0].toInt()
                    val requestCounter   = entryId.split("-")[1].toInt()

                    for(entry in (item as RedisValue.StreamValue).entries) {
                        val entryTimemilis = entry.id.split("-")[0].toInt()
                        val entryCounter   = entry.id.split("-")[1].toInt()

                        if((entryTimemilis > requestedTimeMilis) || (entryTimemilis == requestedTimeMilis && entryCounter > requestCounter)) {
                            tempArr.add(entry)
                        }
                    }

                }

                result.add(tempArr)




            }
        }

        return result
    }

    fun handleIncrement(key: String) : Int {
        synchronized(storage) {
            val item = storage[key]

            if (item is RedisValue.StringValue) {
                val value = (item as RedisValue.StringValue).value.toInt()
                storage[key] = RedisValue.StringValue((value + 1).toString())
                return value + 1
            }

            storage[key] = RedisValue.StringValue("1")
            return 1
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