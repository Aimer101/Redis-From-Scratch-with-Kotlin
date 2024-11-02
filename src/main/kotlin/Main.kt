import java.net.ServerSocket
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.Socket

fun main(args: Array<String>) {
    RDB().createPersistence(args)

    var serverSocket = ServerSocket(DBConfig.port)
    serverSocket.reuseAddress = true
    println("Server started, waiting for connections...")

    val connection = Connection()

    while (true) {
        val client = serverSocket.accept()
        println("Client connected: ${client.inetAddress}")

        Thread {
            connection.onConnect(client)
        }.start()
    }
}