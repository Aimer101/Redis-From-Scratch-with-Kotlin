sealed class DecodeValue {
    data class SimpleStringValue(val value:String): DecodeValue()
    data class BulkStringValue(val value:String): DecodeValue()
    data class IntegerValue(val value:Int): DecodeValue()
    data class ErrorValue(val value:String): DecodeValue()
    data class OutOfIndex(val value: Int) : DecodeValue()
}


class Resp {
    companion object {
        val OK = "OK"
        val QUEUED = "QUEUED"
        val ERROR = "-1"
        val STRING = "string"
        val NONE = "none"
        val PONG = "PONG"
        val PING = "PING"
        val OUTOFINDEX = "$-1\r\n"
        val ERR = "-ERR\r\n"
        val STREAM = "stream"

        fun simpleString(value: String): String {
            return "+${value}\r\n"
        }

        fun integer(value: Int): String {
            return ":${value}\r\n"
        }

        fun bulkString(value: String): String {
            return "$${value.length}\r\n${value}\r\n"
        }

        fun fromArrayString(arr : ArrayList<String>) : String {
            var result = "*${arr.size}\r\n"

            for(str in arr) {
                result += bulkString(str)
            }

            return result
        }

        fun fromDecodeValueArray(arr : ArrayList<DecodeValue>) : String {
            var result = "*${arr.size}\r\n"

            for(str in arr) {
                if(str is DecodeValue.SimpleStringValue) {
                    result += simpleString(str.value)
                } else if (str is DecodeValue.BulkStringValue) {
                    result += bulkString(str.value)
                } else if (str is DecodeValue.IntegerValue) {
                    result += integer(str.value)
                } else if (str is DecodeValue.ErrorValue) {
                    result += simpleError(str.value)
                } else if (str is DecodeValue.OutOfIndex) {
                    result += OUTOFINDEX
                }
            }

            logWithTimestamp("array is ${arr}")
            logWithTimestamp("result is ${result}")

            return result


        }

        fun simpleError(value: String): String {
            return "-ERR ${value}\r\n"
        }

        fun forXRangePayload(arr : ArrayList<StreamEntry>) : String {
            var result = "*${arr.size}\r\n"

            for(entry in arr) {
                result += "*2\r\n"
                result += bulkString(entry.id)

                result += "*${entry.fields.size}\r\n"

                // iterate over fields
                for(field in entry.fields) {
                    result += bulkString(field.key)
                    result += bulkString(field.value)
                }
            }

            return result
        }

        fun forXReadPayload(keys:List<String>, arr : ArrayList<ArrayList<StreamEntry>>) : String {
            if(arr[0].isEmpty()) {
                return OUTOFINDEX
            }
            var result = "*${keys.size}\r\n"

            for ( i in 0 until keys.size) {
                val key = keys[i]
                val entry = arr[i]

                result += "*2\r\n"
                result += bulkString(key)
                result += forXRangePayload(entry)
            }

            return result
        }
    }
}