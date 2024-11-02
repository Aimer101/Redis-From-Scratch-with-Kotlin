import java.nio.file.Paths

object DBConfig {

    var dir: String = "/tmp/redis-files"
    var dbfilename: String = "dump.rdb"
    var isConfigured = false
    var networkPort = 6379
    var master : Master? = null
    val masterReplId: String = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
    val masterReplOffset: Int = 0

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
        val replicaClient = ReplicaClient(masterHost, masterPort)
        replicaClient.ping()
        replicaClient.replconf()
        replicaClient.psync()
    }

    fun getDbFilePath(): String {
        val dbFilePath = Paths.get(dir, dbfilename).toString()

        return dbFilePath
    }

    fun getRoleInfo(): String {
        val role = if (master == null) "master" else "slave"
        return "role:$role"
    }

    fun getMasterReplIdInfo(): String {
        return "master_replid:${this.masterReplId}"
    }


    fun getMasterReplOffsetInfo(): String {
        return "master_repl_offset:${this.masterReplOffset}"
    }

    fun getInfo(): String {
        return """
            ${getRoleInfo()}
            ${getMasterReplIdInfo()}
            ${getMasterReplOffsetInfo()}
        """.trimIndent()
    }
}

data class Master(
    val host: String, 
    val port: Int, 
    val masterReplId: String = "", 
    val masterReplOffset: Int = 0
)