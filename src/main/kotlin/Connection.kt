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
                logWithTimestamp("Raw command received: $request")
                val commandParts = RedisRequestProcessor().procesConcurrentRequest(request)
                var sendEmptyRDB = false

                for(requestParts in commandParts) {
                    if(requestParts.isEmpty()) {
                        outputClient.write("-ERR\r\n".toByteArray())
                    } else if (requestParts[0].uppercase() == Command.PING.value) {
                        outputClient.write("+PONG\r\n".toByteArray())
                    } else if (requestParts[0].uppercase() == Command.ECHO.value) {
                        for (i in 1 until requestParts.size) {
                            logWithTimestamp(requestParts[i])
                            outputClient.write("+${requestParts[i]}\r\n".toByteArray())
                        }
                    } else if (requestParts[0].uppercase() == Command.SET.value) {
                        var expiry: Int? = null

                        if(requestParts.size == 5 && requestParts[4].toIntOrNull() != null && requestParts[3].uppercase() == "PX") {
                            expiry = requestParts[4].toInt()
                        }

                        Storage.set(requestParts[1], requestParts[2], expiry)

                        // send to replicas
                        ReplicaSocket.set(requestParts[1], requestParts[2], expiry)


                        logWithTimestamp("set ${requestParts[1]} ${requestParts[2]}")

                        outputClient.write("+OK\r\n".toByteArray())

                    } else if (requestParts[0].uppercase() == Command.GET.value) {
                        logWithTimestamp("get ${requestParts[1]}")

                        var res : String? = null

                        if(DBConfig.isConfigured) {
                            res = RDB().getValue(requestParts[1])
                        } else {
                            res = Storage.get(requestParts[1])
                        }

                        if(res == null) {
                            // Thread.sleep(1500)

                            outputClient.write("$-1\r\n".toByteArray())
                        } else {
                            outputClient.write("$${res.length}\r\n".toByteArray())
                            outputClient.write("${res}\r\n".toByteArray())
                        }
                    } else if (requestParts[0].uppercase() == Command.CONFIG.value) {
                        if (requestParts[1].uppercase() == ArgCommand.GET.value) {
                            // minutes the CONFIG and GET Command take up
                            // times 2 because each value need to be return together with its key
                            val arrSize = (requestParts.size - 2) * 2
                            outputClient.write("*${arrSize}\r\n".toByteArray())

                            for (i in 2 until requestParts.size) {
                                outputClient.write("$${requestParts[i].length}\r\n".toByteArray())
                                outputClient.write("${requestParts[i].lowercase()}\r\n".toByteArray())
                                val value : String = DBConfig.get(requestParts[i]) ?: ""
                                outputClient.write("$${value.length}\r\n".toByteArray())
                                outputClient.write("${value}\r\n".toByteArray())
                            }

                        }
                    } else if (requestParts[0].uppercase() == Command.KEYS.value) {
                        val listOfKeys = RDB().getAllKeysMatchingPattern(requestParts[1])
                        logWithTimestamp( "List of keys: $listOfKeys" )
                        // iterate and build the resp output for list of keys
                        outputClient.write("*${listOfKeys.size}\r\n".toByteArray())

                        for(key in listOfKeys) {
                            outputClient.write("$${key.length}\r\n".toByteArray())
                            outputClient.write("${key}\r\n".toByteArray())
                        }
                    } else if (requestParts[0].uppercase() == Command.INFO.value) {
                        if(requestParts[1].uppercase() == ArgCommand.REPLICATION.value) {
                            val response = DBConfig.getInfo()

                            outputClient.write("$${response.length}\r\n".toByteArray())
                            outputClient.write("$response\r\n".toByteArray())

                            // Debug prints for verification
                            logWithTimestamp("Combined response:\n$response")

                        } else {
                            outputClient.write("$-1\r\n".toByteArray())
                        }
                    } else if (requestParts[0].uppercase() == Command.REPLCONF.value) {
                        outputClient.write("+OK\r\n".toByteArray())
                    } else if (requestParts[0].uppercase() == Command.PSYNC.value) {
                        outputClient.write("+FULLRESYNC ${DBConfig.masterReplId} 0\r\n".toByteArray())
                        sendEmptyRDB = true
                        ReplicaSocket.addSocket(socket)
                    }

                    outputClient.flush()

                    if(sendEmptyRDB) {
                        val rdbBytes = RDB.EMPTY_RDB.decodeHexString()

                        val message = "$" + rdbBytes.size + "\r\n" + String(rdbBytes, Charsets.ISO_8859_1)

                        outputClient.write(message.toByteArray(Charsets.ISO_8859_1))
                        outputClient.flush()
                    }
                }
            }
        } catch (e: Exception) {
            logWithTimestamp("Error handling client: ${e.message}")
        } finally {
            inputClient.close()
            outputClient.close()
            socket.close()
        }
    }
}