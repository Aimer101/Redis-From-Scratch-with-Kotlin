import java.net.Socket
import java.io.InputStreamReader
import java.io.BufferedReader
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class MasterReplicaConnector (val socket: Socket, private val id: Int){
    private val latestAckResponse: AtomicLong = AtomicLong(0L)
    private val messageQueue = ArrayBlockingQueue<String>(100)

    private val inputClient = socket.getInputStream()
    private val outputClient = socket.getOutputStream()

    fun getLatestAckResponse(): Long {
        return latestAckResponse.get()
    }

    init {
        Thread{
            logMasterReplicatorConnectorWithTimestamp("Starting process queue", id)
            processQueue()
        }.start()

        syncWithReplica()
    }

    private fun syncWithReplica() {
        outputClient.write("+FULLRESYNC ${DBConfig.masterReplId} 0\r\n".toByteArray())
        val rdbBytes = RDB.EMPTY_RDB.decodeHexString()
        val message = "$" + rdbBytes.size + "\r\n" + String(rdbBytes, Charsets.ISO_8859_1)
        outputClient.write(message.toByteArray(Charsets.ISO_8859_1))
        outputClient.flush()

    }

    fun enqueueCommand(command: String) {
        messageQueue.offer(command)
    }

    private fun processQueue() {
        while (true) {
            try {
                val command = messageQueue.poll()

                if (command != null) {
                    logMasterReplicatorConnectorWithTimestamp("Enque a command $command", id)
                    outputClient.write(command.toByteArray())
                    outputClient.flush()
                }
            } catch (e: Exception) {
                logMasterReplicatorConnectorWithTimestamp("Error in processQueue: ${e.message}", id)
            }
        }
    }


    fun handleAckReceived(socketAck: Long) {
        latestAckResponse.set(socketAck)
    }


}