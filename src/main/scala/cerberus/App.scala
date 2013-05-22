package cerberus

import cerberus.exec.DRMAAJobService
import cerberus.exec.LocalJobService

import java.net.{InetAddress, ServerSocket, Socket}
import java.io._

object App {
  def main(args: Array[String]) {
    println("Local: Hello World!")

    val port = 1234
    val server = new ServerSocket(1234)

    val qsub = new DRMAAJobService
    //val qsub = new LocalJobService
    val localhost = InetAddress.getLocalHost.getCanonicalHostName
    if(qsub.isInstanceOf[DRMAAJobService]) {
      assert(localhost != "localhost")
    }
    val jobId = qsub.spawnJob("cerberus.HelloWorld", Array(localhost, port.toString))
    println("spawned "+jobId)

    val client = server.accept()
    val in = new BufferedReader(new InputStreamReader(client.getInputStream()))

    println(in.readLine())

    client.close()
    server.close()
  }
}

object HelloWorld {
  def main(args: Array[String]) {
    val addr = InetAddress.getByName(args(0))
    val port = args(1).toInt

    val skt = new Socket(addr, port)
    val ps = new PrintStream(skt.getOutputStream())

    ps.println("Remote: Hello World!")

    skt.close()
  }
}

