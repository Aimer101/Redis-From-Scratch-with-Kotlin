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

                        outputClient.write(Resp.ERR.toByteArray())

                    } else if (requestParts[0].uppercase() == Command.PING.value) {

                        outputClient.write(Resp.simpleString(Resp.PONG).toByteArray())

                    } else if (requestParts[0].uppercase() == Command.ECHO.value) {

                        for (i in 1 until requestParts.size) {
                            logWithTimestamp(requestParts[i])
                            outputClient.write(Resp.simpleString(requestParts[i]).toByteArray())
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

                        outputClient.write(Resp.simpleString(Resp.OK).toByteArray())

                    } else if (requestParts[0].uppercase() == Command.GET.value) {

                        logWithTimestamp("get ${requestParts[1]}")

                        var res : String? = null

                        if(DBConfig.isConfigured) {
                            res = RDB().getValue(requestParts[1])
                        } else {
                            res = Storage.get(requestParts[1])
                        }

                        if(res == null) {
                            outputClient.write(Resp.OUTOFINDEX.toByteArray())
                        } else {
                            outputClient.write(Resp.bulkString(res).toByteArray());
                        }
                    } else if (requestParts[0].uppercase() == Command.CONFIG.value) {
                        if (requestParts[1].uppercase() == ArgCommand.GET.value) {
                            val tempArr: ArrayList<String> = ArrayList()

                            for (i in 2 until requestParts.size) {
                                tempArr.add(requestParts[i])
                                tempArr.add(DBConfig.get(requestParts[i]) ?: "")
                            }

                            outputClient.write(Resp.fromArrayString(tempArr).toByteArray())
                        }
                    } else if (requestParts[0].uppercase() == Command.KEYS.value) {
                        val listOfKeys = RDB().getAllKeysMatchingPattern(requestParts[1])

                        val tempArr: ArrayList<String> = ArrayList()

                        for(key in listOfKeys) {
                            tempArr.add(key)
                        }

                        outputClient.write(Resp.fromArrayString(tempArr).toByteArray())
                    } else if (requestParts[0].uppercase() == Command.INFO.value) {
                        if(requestParts[1].uppercase() == ArgCommand.REPLICATION.value) {
                            val response = DBConfig.getInfo()

                            outputClient.write(Resp.bulkString(response).toByteArray());

                            logWithTimestamp("Combined response:\n$response")
                        } else {
                            outputClient.write(Resp.OUTOFINDEX.toByteArray())
                        }
                    }
                    else if (requestParts[0].uppercase() == Command.REPLCONF.value) {
                        if(requestParts[1].uppercase() == ArgCommand.ACK.value) {
                            ReplicaService.handleAckReceived(socket ,requestParts[2].toLong())
                        }else {
                            outputClient.write(Resp.simpleString(Resp.OK).toByteArray())
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

                        outputClient.write(Resp.integer(numReplicaAcked).toByteArray())
                    } else if (requestParts[0].uppercase() == Command.TYPE.value) {
                        val key = requestParts[1]
                        val type = Storage.getType(key)
                        outputClient.write(Resp.simpleString(type).toByteArray())

                    } else if (requestParts[0].uppercase() == Command.XADD.value) {
                        val keyName  = requestParts[1]

                        var entryId  = requestParts[2]

                        if (entryId == "*") {
                            entryId = "${System.currentTimeMillis()}-*"
                        }

                        val validationResult = Storage.validateStreamId(keyName, entryId)

                        if (validationResult != null) {
                            outputClient.write(validationResult.toByteArray())
                        } else {
                            val tempHashMap = HashMap<String, String>()

                            var i = 3

                            while(i < requestParts.size) {
                                tempHashMap[requestParts[i]] = requestParts[i+1]
                                i+=2
                            }

                            val res = Storage.handleXadd(keyName, entryId ,tempHashMap)

                            outputClient.write(Resp.bulkString(res).toByteArray())
                        }
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