import java.net.Socket

class Connection {

    private val messageQueue = ArrayList<List<String>>(100)

    private fun executeMessageQueue() : ArrayList<DecodeValue> {
        val result = ArrayList<DecodeValue>()

        while(messageQueue.isNotEmpty()) {
            val requestParts = messageQueue.removeAt(0)

            when(requestParts[0].uppercase()) {
                Command.SET.value -> {
                    var expiry: Int? = null

                    if(requestParts.size == 5 && requestParts[4].toIntOrNull() != null && requestParts[3].uppercase() == "PX") {
                        expiry = requestParts[4].toInt()
                    }

                    Storage.set(requestParts[1], requestParts[2], expiry)

                    result.add(DecodeValue.SimpleStringValue(Resp.OK))
                }

                Command.INCR.value -> {
                    val key = requestParts[1]

                    try {
                        val res = Storage.handleIncrement(key)
                        result.add(DecodeValue.IntegerValue(res))
                    } catch (e: Exception) {
                        result.add(DecodeValue.ErrorValue(e.message!!))
                    }
                }

                Command.GET.value -> {
                    var res : String? = Storage.get(requestParts[1])

                    if(res == null) {
                        result.add(DecodeValue.OutOfIndex(-1))
                    } else {
                        result.add(DecodeValue.BulkStringValue(res))
                    }
                }
            }
        }

        return result
    }

    fun onConnect(socket: Socket) {
        val inputClient = socket.getInputStream()
        val outputClient = socket.getOutputStream()
        var bytesRead: Int
        val buffer = ByteArray(1024);
        var lastWriteMessageOffset = 0L // record offset of the latest write opeartion
        var isMultiCommand = false

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

                    } else if (isMultiCommand && requestParts[0].uppercase() != Command.EXEC.value) {

                        messageQueue.add(requestParts)
                        outputClient.write(Resp.simpleString(Resp.QUEUED).toByteArray())

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
                    } else if (requestParts[0].uppercase() == Command.XRANGE.value) {
                        val keyName     = requestParts[1]
                        val start       = requestParts[2]
                        val end         = requestParts[3]

                        val res = Storage.handleXRange(keyName, start, end)

                        outputClient.write(Resp.forXRangePayload(res).toByteArray())
                    } else if (requestParts[0].uppercase() == Command.XREAD.value) {

                        if(requestParts[1].uppercase() == ArgCommand.STREAMS.value) {
                            val args = requestParts.subList(2, requestParts.size)

                            val midPoint = args.size / 2

                            // names is up to args array
                            val keyNames = args.subList(0, midPoint)
                            val entryIds = args.subList(midPoint, args.size)


                            val res = Storage.handleXRead(keyNames, entryIds)

                            outputClient.write(Resp.forXReadPayload(keyNames,res).toByteArray())
                        } else if (requestParts[1].uppercase() == ArgCommand.BLOCK.value) {
                            logWithTimestamp("Sleeping for ${requestParts[2]}")

                            val startTime   = System.currentTimeMillis()
                            val timeOut     = requestParts[2].toLong()

                            val args = requestParts.subList(4, requestParts.size)

                            val midPoint = args.size / 2

                            // names is up to args array
                            val keyNames = args.subList(0, midPoint)
                            var entryIds = ArrayList(args.subList(midPoint, args.size))

                            for(i in 0 until keyNames.size) {
                                val key = keyNames[i]
                                val entryId = entryIds[i]

                                if(entryId == "$") {
                                    val streamEntries = Storage.getStreamEntries(key)

                                    if(streamEntries != null) {

                                        if(!streamEntries.isEmpty()) {
                                            entryIds[i] = streamEntries.last().id
                                        }
                                    }
                                }
                            }

                            var res = Storage.handleXRead(keyNames, entryIds.toList())

                            if(timeOut == 0L) {
                                while(res[0].isEmpty()) {
                                    res = Storage.handleXRead(keyNames, entryIds)
                                }

                            } else {
                                while(System.currentTimeMillis() - startTime <= timeOut) {
                                    res = Storage.handleXRead(keyNames, entryIds)
                                }
                            }

                            outputClient.write(Resp.forXReadPayload(keyNames,res).toByteArray())

                        }
                    } else if (requestParts[0].uppercase() == Command.INCR.value) {
                            val key = requestParts[1]

                            try {
                                val res = Storage.handleIncrement(key)
                                outputClient.write(Resp.integer(res).toByteArray())
                            } catch (e: Exception) {
                                outputClient.write(Resp.simpleError(e.message!!).toByteArray())
                            }


                    } else if (requestParts[0].uppercase() == Command.MULTI.value) {
                        isMultiCommand = true
                        outputClient.write(Resp.simpleString(Resp.OK).toByteArray())
                    } else if (requestParts[0].uppercase() == Command.EXEC.value) {
                        if(!isMultiCommand) {
                            outputClient.write(Resp.simpleError("EXEC without MULTI").toByteArray())
                        } else {
                            isMultiCommand = false

                            val res = executeMessageQueue()
                            logWithTimestamp("$res")
                            outputClient.write(Resp.fromDecodeValueArray(res).toByteArray())
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