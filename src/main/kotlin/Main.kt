import java.net.ServerSocket
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStreamWriter

fun main(args: Array<String>) {
    var serverSocket = ServerSocket(6379)
    println("Server started, waiting for connections...")

    val client = serverSocket.accept()
    println("Client connected: ${client.inetAddress}")

    val reader = BufferedReader(InputStreamReader(client.getInputStream()))
    val writer = OutputStreamWriter(client.getOutputStream())

    // val command = reader.readLine()
    // println("Received command: $command")

    // writer.write("+PONG\r\n")
    // writer.flush()
    // client.close()

    try {
        var command = reader.readLine()
        do {
            if (command.trim().uppercase() == "PING") {
                writer.write("+PONG\r\n")
                writer.flush()
            }
            command = reader.readLine()
        } while (command != null)
     } catch (e: Exception) {
            println("Error: $e")
     } finally {
          client.close()
     }
}
