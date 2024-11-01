import java.net.ServerSocket

fun main(args: Array<String>) {
    var serverSocket = ServerSocket(6379)
    println("Server started, waiting for connections...")

    val client = serverSocket.accept()
    println("Client connected: ${client.inetAddress}")

    val outputClient = client.getOutputStream()
    outputClient.write("+PONG\r\n".toByteArray())
    println("accepted new connection")
}
