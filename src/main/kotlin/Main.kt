import java.net.ServerSocket
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.Socket 

fun main(args: Array<String>) {
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
                Storage.set(requestParts[1], requestParts[2])
                outputClient.write("+OK\r\n".toByteArray())
            } else if (requestParts[0].uppercase() == Commands.GET.value) {
                val value = Storage.get(requestParts[1])
                if (value == null) {
                    outputClient.write("$-1\r\n".toByteArray())
                } else {
                    outputClient.write("+$value\r\n".toByteArray())
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
