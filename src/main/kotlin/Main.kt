import java.net.ServerSocket
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.Socket
import java.nio.file.Files
import java.nio.file.Paths
import java.io.IOException
import java.io.FileInputStream
import java.io.FileOutputStream

fun main(args: Array<String>) {
    createRDBPersistence(args)

    var serverSocket = ServerSocket(6379)
    serverSocket.reuseAddress = true
    println("Server started, waiting for connections...")

    while (true) {
        val client = serverSocket.accept()
        println("Client connected: ${client.inetAddress}")

        Thread {
            handleClient(client)
        }.start()
    }
}

fun handleClient(client : Socket) {
    val inputClient = client.getInputStream()
    val outputClient = client.getOutputStream()
    var bytesRead: Int
    val buffer = ByteArray(1024);

    try {
        while(client.isConnected) {
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
                var expire: Int? = null

                if(requestParts.size == 5 && requestParts[4].toIntOrNull() != null && requestParts[3].uppercase() == "PX") {
                    expire = requestParts[4].toInt()
                }

                Storage.set(requestParts[1], requestParts[2], expire)
                outputClient.write("+OK\r\n".toByteArray())
            } else if (requestParts[0].uppercase() == Commands.GET.value) {
                val value = Storage.get(requestParts[1])
                if (value == null) {
                    outputClient.write("$-1\r\n".toByteArray())
                } else {
                    outputClient.write("+$value\r\n".toByteArray())
                }
            } else if (requestParts[0].uppercase() == Commands.CONFIG.value) {
                if (requestParts[1].uppercase() == Commands.GET.value) {
                    val arrSize = requestParts.size - 2
                    outputClient.write("*${arrSize * 2}\r\n".toByteArray())

                    for (i in 2 until requestParts.size) {
                        outputClient.write("$${requestParts[i].length}\r\n".toByteArray())
                        outputClient.write("${requestParts[i].lowercase()}\r\n".toByteArray())
                        val value : String = ServerConfig.get(requestParts[i]) ?: ""
                        outputClient.write("$${value.length}\r\n".toByteArray())
                        outputClient.write("${value}\r\n".toByteArray())
                    }
                }
            } else if (requestParts[0].uppercase() == Commands.KEYS.value) {
                val listOfKeys = getAllKeysMatchingPattern(requestParts[1])

                outputClient.write("*${listOfKeys.size}\r\n".toByteArray())

                for (key in listOfKeys) {
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
        client.close()
    }
}

fun createRDBPersistence(args: Array<String>) {
    var dir: String = "/tmp/redis-files"
    var dbfilename: String = "dump.rdb"

    for(i in args.indices){
        when(args[i]){
            "--dir" -> dir = args[i + 1]
            "--dbfilename" -> dbfilename = args[i + 1]
        }
    }

    ServerConfig.set("DIR", dir)
    ServerConfig.set("DBFILENAME", dbfilename)

    // Create directory if it doesn't exist
    val dirPath = Paths.get(dir)
    if (!Files.exists(dirPath)) {
        Files.createDirectories(dirPath)
        println("Directory created: $dir")
    } else {
        println("Directory already exists: $dir")
    }

    // Create database file
    val dbFilePath = Paths.get(dir, dbFilename)
    if (!Files.exists(dbFilePath)) {
        Files.createFile(dbFilePath)
        println("Database file created: $dbFilePath")
    } else {
        println("Database file already exists: $dbFilePath")
    }

}

fun getAllKeysMatchingPattern(pattern:String) : List<String> {
    val dbPath = Helpers().getDbFilePath()
    val matchingKeys= mutableListOf<String>()

    val regexPattern = pattern.replace("*", ".*").toRegex()

    try {

        FileInputStream(dpPath). use { fis ->
            // 1. read and verify header "REDIS0011"
            val header = ByteArray(9)
            if (fis.read(header) != 9) return@use matchingKeys
            val headerStr = String(header)
            if (!headerStr.startsWith("REDIS")) return@use matchingKeys

            var byteRead: Int

            while (fis.read().also { byteRead = it } != -1) {
                when(byteRead.toByte()) {
                    0xFA.toByte() -> {
                        // Skip metadata section
                        val nameLength = fis.read() // read name length
                        fis.skip(nameLength.toLong())  // skip  - The name of the metadata attribute (string encoded): "redis-ver".
                        val valueLength = fis.read() // read value length
                        fis.skip(valueLength.toLong()) // skip - The value of the metadata attribute (string encoded): "6.0.16".
                    }
                    0XFE.toByte() -> {
                        val dbIndex = fis.read() // read database index

                        val marker = fis.read() //read hashtable info

                        if(marker.toByte() == 0xFB.toByte()) {
                            val keyValueHashSize = fis.read() // The size of the hash table that stores the keys and values (size encoded)
                            val keyExpiry = fis.read() // The size of the hash table that stores the expires of the keys (size encoded)

                            while (fis.read().also { byteRead = it } != -1) {
                                when (byteRead.toByte()) {
                                    0x00.toByte() -> { // type string
                                        val keyLength = fis.read()
                                        if (keyLength == -1) return

                                        val keyBytes = ByteArray(keyLength)
                                        if (fis.read(keyBytes) != keyLength) return

                                        val key = String(keyBytes)

                                        if (key.matches(regexPattern)) {
                                            matchingKeys.add(key)
                                        }

                                        // Skip value
                                        val valueLength = fis.read()
                                        if (valueLength == -1) return
                                        fis.skip(valueLength.toLong())
                                    }

                                    0xFC.toByte() -> { // has an expiry timestamp (ms in Unix time, stored as an 8-byte unsigned long, in Little Endian order)
                                        fis.skip(8)  // Skip timestamp
                                    }

                                    0xFD.toByte() -> { // has an expiry timestamp (seconds in Unix time stored as an 4-byte unsigned integer)
                                        fis.skip(4)
                                    }
                                }
                            }
                        }

                    }
                }
            }

        }

    } catch (e : IOException) {
        println("Error while reading RDB file: $e")
    }

    return matchingKeys
}