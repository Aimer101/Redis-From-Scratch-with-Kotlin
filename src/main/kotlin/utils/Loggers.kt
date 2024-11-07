import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

fun logWithTimestamp(message: String) {
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    println("[$timestamp][${DBConfig.getRoleInfo()}] $message")
}

fun logPropagationWithTimestamp(message: String) {
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    println("[$timestamp][propagation][${DBConfig.getRoleInfo()}] $message")
}

fun logMasterReplicatorConnectorWithTimestamp(message: String, id: Int) {
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    println("[$timestamp][master-replicator-connector][${DBConfig.getRoleInfo()}][id=$id] $message")
}