import java.net.ServerSocket
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.Socket 

fun main(args: Array<String>) {
    var dir: String? = null
    var dbfilename: String? = null

    for(i in args.indices){
        when(args[i]){
            "--dir" -> dir = args[i + 1]
            "--dbfilename" -> dbfilename = args[i + 1]
        }
    }

    ServerConfig.set("DIR", dir ?: "")
    ServerConfig.set("DBFILENAME", dbfilename ?: "")

    var serverSocket = ServerSocket(6379)
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
            System.out.println("requestParts size: " + requestParts.size)
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
                var expire = 0

                if(requestParts.size == 5 && requestParts[4].toIntOrNull() != null && requestParts[3] == "px") {
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
                        outputClient.write("+${requestParts[i].lowercase()}\r\n".toByteArray())
                        val value : String = ServerConfig.get(requestParts[i]) ?: ""
                        outputClient.write("$${value.length}\r\n".toByteArray())
                        outputClient.write("+${value}\r\n".toByteArray())
                    }
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
