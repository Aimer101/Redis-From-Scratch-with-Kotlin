class RedisRequestProcessor {
    private val ASTERISK = '*';
    fun processRequest(request: String): List<String> {
        // Process the request and create a RedisRequest object
        // This is a placeholder implementation
        // *2\r\n$4\r\nECHO\r\n$9\r\nraspberry
        val parsedRequestParts = ArrayList<String>();
        if(request.startsWith(ASTERISK)){
            val requestParts = request.split("\r\n")
            // [*2, $4, ECHO, $9, raspberry]

            // to get total args
            var numParts = requestParts[0].replace("*","").toInt()

            System.out.println("nArgs: "+numParts)

            var index = 2;

            while(numParts > 0){
                parsedRequestParts.add(requestParts[index])
                index+=2;
                numParts--;
            }
        }
        return parsedRequestParts;
    }
}