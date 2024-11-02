import java.net.Socket

class Connection {

    fun onConnect(socket: Socket) {
        val inputClient = socket.getInputStream()
        val outputClient = socket.getOutputStream()
        var bytesRead: Int
        val buffer = ByteArray(1024);

        try {
            while(socket.isConnected) {
                bytesRead = inputClient.read(buffer)

                if (bytesRead == -1) {
                    break
                }

                val request = buffer.copyOfRange(0, bytesRead).toString(Charsets.UTF_8);
                val requestParts = RedisRequestProcessor().processRequest(request)
                println("requestParts size: " + requestParts.size)

                if(requestParts.isEmpty()) {
                    outputClient.write("-ERR\r\n".toByteArray())
                } else if (requestParts[0].uppercase() == Commands.PING.value) {
                    outputClient.write("+PONG\r\n".toByteArray())
                } else if (requestParts[0].uppercase() == Commands.ECHO.value) {
                    for (i in 1 until requestParts.size) {
                        println(requestParts[i])
                        outputClient.write("+${requestParts[i]}\r\n".toByteArray())
                    }
                } else if (requestParts[0].uppercase() == Commands.SET.value) {
                    var expiry: Int? = null

                    if(requestParts.size == 5 && requestParts[4].toIntOrNull() != null && requestParts[3].uppercase() == "PX") {
                        expiry = requestParts[4].toInt()
                    }

                    Storage.set(requestParts[1], requestParts[2], expiry)
                    outputClient.write("+OK\r\n".toByteArray())
                } else if (requestParts[0].uppercase() == Commands.GET.value) {
                    val value = Storage.get(requestParts[1])

                    if(value == null) {
                        outputClient.write("$-1\r\n".toByteArray())
                    } else {
                        outputClient.write("+$value\r\n".toByteArray())

                    }
                } else if (requestParts[0].uppercase() == Commands.CONFIG.value) {
                    if (requestParts[1].uppercase() == Commands.GET.value) {
                         // minutes the CONFIG and GET commands take up
                         // times 2 because each value need to be return together with its key
                        val arrSize = (requestParts.size - 2) * 2
                        outputClient.write("*${arrSize}\r\n".toByteArray())

                        for (i in 2 until requestParts.size) {
                            outputClient.write("$${requestParts[i].length}\r\n".toByteArray())
                            outputClient.write("${requestParts[i].lowercase()}\r\n".toByteArray())
                            val value : String = ServerConfig.get(requestParts[i]) ?: ""
                            outputClient.write("$${value.length}\r\n".toByteArray())
                            outputClient.write("${value}\r\n".toByteArray())
                        }

                    }
                } else if (requestParts[0].uppercase() == Commands.KEYS.value) {
                    val listOfKeys = RDB().getAllKeysMatchingPattern(requestParts[1])
                    println( "List of keys: $listOfKeys" )
                    // iterate and build the resp output for list of keys
                    outputClient.write("*${listOfKeys.size}\r\n".toByteArray())

                    for(key in listOfKeys) {
                        outputClient.write("$${key.length}\r\n".toByteArray())
                        outputClient.write("${key}\r\n".toByteArray())
                    }
                }

                outputClient.flush()
            }
        } catch (e: Exception) {
            println("Error handling client: ${e.message}")
        } finally {
            inputClient.close()
            outputClient.close()
            socket.close()
        }
        
    }
}