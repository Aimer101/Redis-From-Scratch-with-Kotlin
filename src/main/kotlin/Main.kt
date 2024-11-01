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
    val reader = BufferedReader(InputStreamReader(client.getInputStream()))
    val writer = OutputStreamWriter(client.getOutputStream())

    try {
        val command = reader.readLine() ?: ""
        println("Received command: $command")

        if (command.trim().uppercase() == "PING") {
            writer.write("+PONG\r\n")
            writer.flush()
        }

    } catch(e: Exception) {
        println("Error: $e")
    } finally {
        client.close()
    }

}
