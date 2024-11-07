import java.net.Socket
import java.io.PrintWriter
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.ServerSocket
// import java.util.concurrent.ArrayBlockingQueue
// import java.util.concurrent.TimeUnit

object ReplicaClient {
    private lateinit var socket: Socket
    private lateinit var outputClient: PrintWriter
    private lateinit var request: BufferedReader
    private var isReady = false
    @Volatile private var offset = 0


    fun connectToMaster(host: String, port: Int) {
        socket          = Socket(host, port)
        outputClient    = PrintWriter(socket.getOutputStream(), true)
        request         = BufferedReader(InputStreamReader(socket.getInputStream()))

        // Thread{
        //     processQueue()
        // }.start()


        Thread {
            ping()
            replconf()
            psync()
            startListening()
        }.start()


    }

    private fun recordProcessedRequestBytes(request: List<String>) {
        logPropagationWithTimestamp("recording the processed request bytes" + request)

        val totalArgs = request.size.toString().length
        logPropagationWithTimestamp("total args bytes are " + totalArgs)

        synchronized(offset) {

            offset += totalArgs + 1 + 2 // 1 for *, 2 for \r\n

            for(arg in request) {
                logPropagationWithTimestamp("processing arg " + arg)

                val lengthOfArg = arg.length.toString().length
                logPropagationWithTimestamp("length of arg length is " + lengthOfArg)

                offset += lengthOfArg + 1 + 2 // 1 for $, 2 for \r\n

                offset += arg.length + 2 // 2 for \r\n
                logPropagationWithTimestamp("actual length of arg  is " + arg.length)

            }
        }

    }

    private fun startListening() {
        val input = socket.getInputStream()
        var bytesRead: Int
        val buffer = ByteArray(1024);

        logPropagationWithTimestamp("Starting replica - master...")

        while(true) {
            if(!isReady) {
                logPropagationWithTimestamp("Waiting for psync...")
                continue
            }
            bytesRead = input.read(buffer)

            if (bytesRead == -1) {
                break
            }

            val request = buffer.copyOfRange(0, bytesRead).toString(Charsets.UTF_8);
            logPropagationWithTimestamp("Raw command received during propagation: $request")
            val requestParts = RedisRequestProcessor().procesConcurrentRequest(request)
            logPropagationWithTimestamp("Request parts during propagation: $requestParts")
            logPropagationWithTimestamp("Request parts size: ${requestParts.size}")

            if(requestParts.isEmpty()) {
                continue
            }

            // iterate over concurrent request parts
            for(commandParts in requestParts) {
                    if(commandParts[0].uppercase() == Command.SET.value) {
                            var expiry: Int? = null

                            if(commandParts.size == 5 && commandParts[4].toIntOrNull() != null && commandParts[3].uppercase() == "PX") {
                                expiry = commandParts[4].toInt()
                            }

                            Storage.set(commandParts[1], commandParts[2], expiry)

                            logPropagationWithTimestamp("set ${commandParts[1]} ${commandParts[2]}")
                    } else if (commandParts[0].uppercase() == Command.REPLCONF.value) {
                        if (commandParts[1].uppercase() == ArgCommand.GETACK.value) {

                            outputClient.print("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$${offset.toString().length}\r\n${offset.toString()}\r\n")
                            outputClient.flush()
                        }

                    }
                    recordProcessedRequestBytes(commandParts)
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
        isReady = true

        // val response = request.readLine()
        // logPropagationWithTimestamp("Response from master for psync: $response")
    }
}