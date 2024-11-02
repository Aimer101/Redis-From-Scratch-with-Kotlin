import java.net.Socket
import java.io.PrintWriter
import java.io.BufferedReader
import java.io.InputStreamReader

class ReplicaClient (private val host : String, private val port : Int) {
    private val socket: Socket = Socket(host, port)
    private val outputClient = PrintWriter(socket.getOutputStream(), true)
    private val request = BufferedReader(InputStreamReader(socket.getInputStream()))

    fun ping () {
        println("Sending ping to master")
        outputClient.print("*1\r\n$4\r\nPING\r\n")
        outputClient.flush()
        println("Ping sent to master")

        val response = request.readLine()
        println("Response from master for ping: $response")
    }

    fun replconf() {
        println("Sending replconf 1 to master")
        outputClient.print("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$${port.toString().length}\r\n${port}\r\n")
        outputClient.flush()
        println("Replconf 1 sent to master")

        var response = request.readLine()
        println("Response from master for replconf 1: $response")

        println("Sending replconf 2 to master")
        outputClient.print("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
        outputClient.flush()
        println("Replconf 2 sent to master")

        response = request.readLine()
        println("Response from master for replconf 2: $response")
    }
}