import java.nio.file.Paths

object DBConfig {

    var dir: String = "/tmp/redis-files"
    var dbfilename: String = "dump.rdb"
    var isConfigured = false
    var networkPort = 6379
    var master : Master? = null
    val masterReplicationId: String = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
    val masterReplicationOffset: Int = 0

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
        this.master = Master(masterHost, masterPort)
    }

    fun getDbFilePath(): String {
        val dbFilePath = Paths.get(dir, dbfilename).toString()

        return dbFilePath
    }

    fun getRole(): String {
        val role = if (master != null) "master" else "slave"
        return "role:$role"
    }

    fun getMasterReplicationId(): String {
        return "master_replid:${this.masterReplicationId}"
    }

    fun getMasterReplicationOffset(): String {
        return "master_repl_offset:${this.masterReplicationOffset}"
    }
}

data class Master(
    val host: String, 
    val port: Int, 
    val masterReplicationId: String = "", 
    val masterReplicationOffset: Int = 0
)