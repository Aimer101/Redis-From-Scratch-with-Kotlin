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

    while(client.isConnected) {
        val request = inputClient.bufferedReader()
        val requestBody = request.readLine() ?: ""
        if(requestBody.isEmpty()) {
            break
        }
        outputClient.write("+PONG\r\n".toByteArray())
        outputClient.flush()
    }
}
