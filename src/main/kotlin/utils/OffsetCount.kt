fun offsetCount (rawRequest: String): Int {
    var offset = 0

    val requests = RedisRequestProcessor().procesConcurrentRequest(rawRequest)

    for(request in requests) {
        logWithTimestamp("Recoding the processed request bytes $request, before is $offset ")
        val totalArgs = request.size.toString().length

        offset += totalArgs + 1 + 2 // 1 for *, 2 for \r\n

        for(arg in request) {

            val lengthOfArg = arg.length.toString().length

            offset += lengthOfArg + 1 + 2 // 1 for $, 2 for \r\n

            offset += arg.length + 2 // 2 for \r\n
        }
        logWithTimestamp("Recoding the processed request bytes $request, after is $offset ")
    }

    return offset
}