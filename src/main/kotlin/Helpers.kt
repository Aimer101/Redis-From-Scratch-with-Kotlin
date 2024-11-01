import java.nio.file.Paths

class Helpers {
    fun getDbFilePath(): String {
        val dir = ServerConfig.get("DIR") ?: "/tmp/redis-files"
        val dbFilename = ServerConfig.get("DBFILENAME") ?: "dump.rdb"
        val dbFilePath = Paths.get(dir, dbFilename).toString()
        return dbFilePath
    }
}