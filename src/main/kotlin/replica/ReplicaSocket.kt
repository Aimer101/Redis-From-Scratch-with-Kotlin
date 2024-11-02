import java.net.Socket


object ReplicaSocket {
    val replicaSockets: MutableList<Socket> = mutableListOf()

    fun addSocket(socket: Socket) {
        replicaSockets.add(socket)
    }

    fun set(key : String, value : String, expire: Int? = null) {
        if(replicaSockets.isEmpty()) {
            return
        }

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


        for(socket in replicaSockets) {

            socket.getOutputStream().write(command.toByteArray())
            socket.getOutputStream().flush()
        }

    }
}