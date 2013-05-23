package cerberus

import cerberus.exec.DRMAAJobService
import cerberus.exec.LocalJobService

import java.net.{InetAddress, ServerSocket, Socket}
import java.io._

sealed trait Message
case class EchoMessage(str: String) extends Message
case class PingMessage() extends Message

object App {
  def main(args: Array[String]) {
    println("Local: Hello World!")

    val AnyPort = 0
    val server = new ServerSocket(AnyPort)
    val port = server.getLocalPort

    val qsub = if(args.size == 1 && args(0) == "drmaa") { 
      new DRMAAJobService
    } else new LocalJobService

    val localhost = InetAddress.getLocalHost.getCanonicalHostName
    if(qsub.isInstanceOf[DRMAAJobService]) {
      assert(localhost != "localhost")
    }
    val jobId = qsub.spawnJob("cerberus.HelloWorld", Array(localhost, port.toString))
    println("spawned "+jobId)

    val client = server.accept()
    val out = new ObjectOutputStream(new DataOutputStream(client.getOutputStream()))
    val in = new BufferedReader(new InputStreamReader(client.getInputStream()))

    val doThisRemotely: PrintStream=>Unit = (ps => { ps.println("Anonymously Remote: Hello World!") })
    out.writeObject(doThisRemotely)
    out.writeObject(EchoMessage("echo foobar"))
    out.writeObject(PingMessage())

    println(in.readLine())
    println(in.readLine())
    println(in.readLine())
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
    
    val in = new ObjectInputStream(new DataInputStream(skt.getInputStream()))
    val func = in.readObject().asInstanceOf[PrintStream=>Unit]

    (0 until 2).foreach(ignored => {
      in.readObject() match {
        case EchoMessage(str) => ps.println(str)
        case PingMessage() => ps.println("Pong")
      }
    })
    
    func(ps)

    skt.close()
  }
}

