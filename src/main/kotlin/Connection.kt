import java.net.Socket

class Connection {

    fun onConnect(socket: Socket) {
        val inputClient = socket.getInputStream()
        val outputClient = socket.getOutputStream()
        var bytesRead: Int
        val buffer = ByteArray(1024);
        var lastWriteMessageOffset = 0L // record offset of the latest write opeartion

        try {
            while(socket.isConnected) {
                bytesRead = inputClient.read(buffer)

                if (bytesRead == -1) {
                    break
                }

                val request = buffer.copyOfRange(0, bytesRead).toString(Charsets.UTF_8);
                logWithTimestamp("Raw command received: $request")
                val commandParts = RedisRequestProcessor().procesConcurrentRequest(request)

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

                        // send to replicas and record the offset
                        lastWriteMessageOffset = ReplicaService.set(requestParts[1], requestParts[2], expiry)


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
                    }
                    else if (requestParts[0].uppercase() == Command.REPLCONF.value) {
                        if(requestParts[1].uppercase() == ArgCommand.ACK.value) {
                            ReplicaService.handleAckReceived(socket ,requestParts[2].toLong())
                        }else {
                            outputClient.write("+OK\r\n".toByteArray())
                        }
                    }
                    else if (requestParts[0].uppercase() == Command.PSYNC.value) {
                        ReplicaService.addSocket(socket)
                        break
                    } else if (requestParts[0].uppercase() == Command.WAIT.value) {
                        val startTime = System.currentTimeMillis()
                        val nReplicas = requestParts[1].toInt()
                        val timeout = requestParts[2].toLong()
                        var numReplicaAcked = 0

                        while(System.currentTimeMillis() - startTime <= timeout) {
                            ReplicaService.sendAck()
                            numReplicaAcked = ReplicaService.nReplicaAcked(lastWriteMessageOffset)

                            if(numReplicaAcked >= nReplicas) {
                                break
                            }
                        }

                        val socketSize = numReplicaAcked
                        outputClient.write(":${socketSize}\r\n".toByteArray())
                    }

                    outputClient.flush()
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