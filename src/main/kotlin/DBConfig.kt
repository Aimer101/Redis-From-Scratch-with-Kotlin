import java.nio.file.Paths

object DBConfig {

    var dir: String = "/tmp/redis-files"
    var dbfilename: String = "dump.rdb"
    var isConfigured = false

    fun get(key: String): String {
        return when(key.uppercase()){
            "DIR" -> dir
            "DBFILENAME" -> dbfilename
            else -> ""
        }
    }

    fun set(key: String, value: String?) {
        isConfigured = true
        when(key.uppercase()){
            "DIR" -> {
                if (value != null) {
                    dir = value
                }
            }
            "DBFILENAME" -> {
                if (value != null) {
                    dbfilename = value
                }
            }
        }
    }

    fun getDbFilePath(): String {
        val dbFilePath = Paths.get(dir, dbfilename).toString()

        return dbFilePath
    }
}