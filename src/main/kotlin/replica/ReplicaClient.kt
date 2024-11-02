import java.net.Socket
import java.io.PrintWriter

class ReplicaClient (host : String, port : Int) {
    private val socket: Socket = Socket(host, port)
    private val outputClient = PrintWriter(socket.getOutputStream(), true)

    fun ping () {
        println("Sending ping to master")
        outputClient.print("*1\r\n$4\r\nPING\r\n")
        outputClient.flush()
        println("Ping sent to master")
    }
}