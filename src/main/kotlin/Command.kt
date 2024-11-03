enum class Command(val value: String) {
    GET("GET"),
    SET("SET"),
    PING("PING"),
    ECHO("ECHO"),
    CONFIG("CONFIG"),
    KEYS("KEYS"),
    INFO("INFO"),
    REPLCONF("REPLCONF"),
    PSYNC("PSYNC"),

}

enum class ArgCommand(val value: String) {
    GET("GET"),
    REPLICATION("REPLICATION"),
    GETACK("GETACK"),
}