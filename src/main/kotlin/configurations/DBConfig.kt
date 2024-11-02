import java.nio.file.Paths

object DBConfig {

    var dir: String = "/tmp/redis-files"
    var dbfilename: String = "dump.rdb"
    var isConfigured = false
    var networkPort = 6379
    var masterHost : String? = null
    var masterPort : Int? = null

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

    fun setPort(networkPort: Int) {
        this.networkPort = networkPort
    }

    fun setMaster(masterHost: String, masterPort: Int) {
        this.masterHost = masterHost
        this.masterPort = masterPort
    }

    fun getDbFilePath(): String {
        val dbFilePath = Paths.get(dir, dbfilename).toString()

        return dbFilePath
    }

    fun getInfo(): String {
        val role = if (masterHost == null && masterPort == null) "master" else "slave"
        return "role:$role"
    }
}