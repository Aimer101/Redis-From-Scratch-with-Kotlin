class RedisRequestProcessor {
    private val ASTERISK = '*';
    fun processRequest(request: String): List<String> {
        // Process the request and create a RedisRequest object
        // This is a placeholder implementation
        // *2\r\n$4\r\nECHO\r\n$9\r\nraspberry\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n
        val parsedRequestParts = ArrayList<String>();
        if(request.startsWith(ASTERISK)){
            val requestParts = request.split("\r\n")
            // [*2, $4, ECHO, $9, raspberry]

            // to get total args
            var numParts = requestParts[0].replace("*","").toInt()

            // System.out.println("nArgs: "+numParts)

            var index = 2;

            while(numParts > 0){
                parsedRequestParts.add(requestParts[index])
                index+=2;
                numParts--;
            }
        }
        return parsedRequestParts;
    }

    fun extractRESPCommand(input: String): String? {
        // Regular expression to match RESP array indicator *<number>
        val regex = Regex("\\*[0-9]+")
        return regex.find(input)?.value
    }

    fun procesConcurrentRequest(request: String): ArrayList<List<String>> {
        // Process the request and create a RedisRequest object
        // This is a placeholder implementation
        // *2\r\n$4\r\nECHO\r\n$9\r\nraspberry\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n
        val parsedRequestParts = ArrayList<List<String>>()
        val rawInput = ArrayList(request.split("\r\n"))

        while(rawInput.isNotEmpty()) {
            val command = rawInput.removeAt(0)
            if(extractRESPCommand(command) == null) {
                continue
            }
            val commandLength = extractRESPCommand(command)!!.replace("*", "").toInt()

            val tempArr: ArrayList<String> = ArrayList()

            for(i in 1..commandLength) {
                tempArr.add(rawInput[1])
                rawInput.removeAt(0)
                rawInput.removeAt(0)
            }

            parsedRequestParts.add(tempArr)
        }
        return parsedRequestParts;
    }
}