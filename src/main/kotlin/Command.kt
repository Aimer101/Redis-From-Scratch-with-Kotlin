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
    WAIT("WAIT"),
    TYPE("TYPE"),
    XADD("XADD"),
    XRANGE("XRANGE"),
    XREAD("XREAD"),
}

enum class ArgCommand(val value: String) {
    GET("GET"),
    REPLICATION("REPLICATION"),
    GETACK("GETACK"),
    ACK("ACK"),
}