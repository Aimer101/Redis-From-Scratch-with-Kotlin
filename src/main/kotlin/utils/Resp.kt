class Resp {
    companion object {
        val OK = "OK"
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

        fun simpleError(value: String): String {
            return "-ERR ${value}\r\n"
        }
    }
}