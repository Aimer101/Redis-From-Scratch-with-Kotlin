import java.nio.file.Paths

object ServerConfig {

    var dir: String = "/tmp/redis-files"
    var dbfilename: String = "dump.rdb"

    fun get(key: String): String {
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

    fun getDbFilePath(): String {
        val dbFilePath = Paths.get(dir, dbFilename).toString()

        return dbFilePath
    }
}