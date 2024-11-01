object ServerConfig {

    var dir: String? = null
    var dbfilename: String? = null

    fun get(key: String): String? {
        return when(key.uppercase()){
            "DIR" -> dir
            "DBFILENAME" -> dbfilename
            else -> null
        }
    }

    fun set(key: String, value: String) {
        when(key.uppercase()){
            "DIR" -> dir = value
            "DBFILENAME" -> dbfilename = value
        }
    }
}