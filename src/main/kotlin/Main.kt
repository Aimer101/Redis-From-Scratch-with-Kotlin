import java.net.ServerSocket
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.Socket

fun main(args: Array<String>) {
    RDB().createPersistence(args)

    var serverSocket = ServerSocket(DBConfig.networkPort)
    serverSocket.reuseAddress = true


    println("${DBConfig.getRoleInfo()} Server started, waiting for connections...")
    // Thread.sleep(2000)


    val connection = Connection()

    while (true) {
        val client = serverSocket.accept()
        println("${DBConfig.getRoleInfo()} Client connected: ${client.inetAddress}")

        Thread {
            connection.onConnect(client)
        }.start()
    }
}