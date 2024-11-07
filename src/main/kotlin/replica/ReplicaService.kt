import java.net.Socket
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock



object ReplicaService {
    val slaveSockets: MutableList<MasterReplicaConnector> = mutableListOf()
    val idGenerator = AtomicInteger(0)

    private val isLatestCommandEqualToSet           = AtomicBoolean(false)
    private val lock                                = ReentrantLock()

    fun addSocket(socket: Socket) {
        slaveSockets.add(MasterReplicaConnector(socket, idGenerator.getAndIncrement()))
    }

    fun set(key : String, value : String, expire: Int? = null) : Long {
        var argLen = 3

        if (expire != null) {
            argLen = 5
        }

        var command = "*${argLen}\r\n" +  // Number of elements in the array
        "$3\r\nSET\r\n" +  // SET command
        "$${key.length}\r\n${key}\r\n" +  // Key
        "$${value.length}\r\n${value}\r\n"      // Value

        if (expire != null) {
            command += "$2\r\npx\r\n"
            command += "$${expire.toString().length}\r\n${expire.toString()}\r\n"
        }

        lock.lock()
        try {
            val currentCommandOffset = offsetCount(command)
            for(socket in slaveSockets) {
                socket.enqueueCommand(command)
            }

            isLatestCommandEqualToSet.set(true)
            return currentCommandOffset.toLong()
        } finally {
            lock.unlock()
        }
    }

    fun handleAckReceived(socket:Socket, offset: Long) {
        for(slaveSocket in slaveSockets) {
            if(slaveSocket.socket == socket) {
                slaveSocket.handleAckReceived(offset)
                break
            }
        }
    }

    fun sendAck() {
        // this command can only be invoke if previous command is a SET
        if (isLatestCommandEqualToSet.get()) {

            val command = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"

            lock.lock()
            try {
                for(socket in slaveSockets) {
                    socket.enqueueCommand(command)
                }
                isLatestCommandEqualToSet.set(false)
            } finally {
                lock.unlock()
            }
        }
    }

    fun nReplicaAcked(messageOffset: Long): Int {
        var result = 0
        for (socket in slaveSockets) {
            if (socket.getLatestAckResponse() >= messageOffset) {
                result++
            }
        }
        return result
    }
}