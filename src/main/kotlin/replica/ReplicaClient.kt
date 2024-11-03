import java.net.Socket
import java.io.PrintWriter
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.ServerSocket
import java.lang.Thread.sleep

object ReplicaClient {
    private lateinit var socket: Socket
    private lateinit var outputClient: PrintWriter
    private lateinit var request: BufferedReader

    fun connectToMaster(host: String, port: Int) {
        socket          = Socket(host, port)
        outputClient    = PrintWriter(socket.getOutputStream(), true)
        request         = BufferedReader(InputStreamReader(socket.getInputStream()))

            ping()
            replconf()
        Thread {
            psync()
        }.start()

        Thread {
            startListening()

        }.start()
    }

    private fun startListening() {
        val input = socket.getInputStream()
        var bytesRead: Int
        val buffer = ByteArray(1024);

        logPropagationWithTimestamp("Starting replica - master...")

        while(true) {
            bytesRead = input.read(buffer)

            if (bytesRead == -1) {
                break
            }

            val request = buffer.copyOfRange(0, bytesRead).toString(Charsets.UTF_8);
            // logPropagationWithTimestamp("Raw command received during propagation: $request")
            val requestParts = RedisRequestProcessor().procesConcurrentRequest(request)

            // iterate over concurrent request parts
            for(commandParts in requestParts) {
                if(!requestParts.isEmpty()) {
                    if (commandParts[0].uppercase() == Command.SET.value) {
                        logPropagationWithTimestamp("processing set command ${commandParts[1]} ${commandParts[2]}")
                        var expiry: Int? = null

                        if(commandParts.size == 5 && commandParts[4].toIntOrNull() != null && commandParts[3].uppercase() == "PX") {
                            expiry = commandParts[4].toInt()
                        }

                        Storage.set(commandParts[1], commandParts[2], expiry)

                        logPropagationWithTimestamp("set ${commandParts[1]} ${commandParts[2]}")
                    } else if (commandParts[0].uppercase() == Command.GET.value) {
                            logPropagationWithTimestamp("get ${commandParts[1]}")

                            var res : String? = null

                            res = Storage.get(commandParts[1])

                            if(res == null) {
                                outputClient.print("$-1\r\n")
                            } else {
                                outputClient.print("$${res.length}\r\n")
                                outputClient.print("${res}\r\n")
                            }
                            outputClient.flush()
                    }
                }
            }


        }
    }


    private fun ping () {
        logPropagationWithTimestamp("Sending ping to master")
        outputClient.print("*1\r\n$4\r\nPING\r\n")
        outputClient.flush()
        logPropagationWithTimestamp("Ping sent to master")

        val response = request.readLine()
        logPropagationWithTimestamp("Response from master for ping: $response")
    }

    private fun replconf() {
        logPropagationWithTimestamp("Sending replconf 1 to master")
        outputClient.print("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n")
        outputClient.flush()
        logPropagationWithTimestamp("Replconf 1 sent to master")

        var response = request.readLine()
        logPropagationWithTimestamp("Response from master for replconf 1: $response")

        logPropagationWithTimestamp("Sending replconf 2 to master")
        outputClient.print("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
        outputClient.flush()
        logPropagationWithTimestamp("Replconf 2 sent to master")

        response = request.readLine()
        logPropagationWithTimestamp("Response from master for replconf 2: $response")
    }

    private fun psync() {
        logPropagationWithTimestamp("Sending psync to master")
        outputClient.print("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        outputClient.flush()
        logPropagationWithTimestamp("Psync sent to master")

        // val response = request.readLine()
        // logPropagationWithTimestamp("Response from master for psync: $response")
    }
}