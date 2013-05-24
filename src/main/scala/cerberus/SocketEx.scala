package cerberus

import cerberus.exec.DRMAAJobService
import cerberus.exec.LocalJobService
import cerberus.exec.Time

import java.net.{InetAddress, ServerSocket, Socket}
import java.io._

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{Try, Success, Failure}

sealed trait Message
case class EchoMessage(str: String) extends Message
case class PingMessage() extends Message
case class QuitMessage() extends Message

sealed trait JobResult
case class JobSuccess() extends JobResult
case class JobFailure() extends JobResult

object IO {
  def cat(data: String, fileName: String) {
    var fp = new PrintWriter(fileName)
    fp.println(data)
    fp.close()
  }
}

object SocketEx {

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
    //val jobId = qsub.spawnJob("cerberus.HelloWorld", Array(localhost, port.toString))
    //println("spawned "+jobId)

    val functions = List(
      { IO.cat("foobar", "foo1.txt") },
      { IO.cat("barfoo", "foo2.txt") },
      { 
        val arr = Array(2,6,5,4,2,13,4,5,6,7,83,4,5,7,100)
        IO.cat(arr.sorted.mkString(","), "sorted.txt")
      }
    )
    var runningJobs = Set[Future[JobResult]]()

    functions.foreach(task => {
      val jobId = qsub.spawnJob("cerberus.Task", Array(localhost, port.toString))
      println("spawned "+jobId)
      
      runningJobs = runningJobs + future {
        val client = server.accept()
        val out = new ObjectOutputStream(new DataOutputStream(client.getOutputStream()))
        val in = new ObjectInputStream(new DataInputStream(client.getInputStream()))
        
        out.writeObject(task)
        in.readObject().asInstanceOf[JobResult]
      }

    })

    // while there is some unfinished job:
    while(runningJobs.exists(!_.isCompleted)) {
      Time.snooze(200)
      println("Running jobs: "+runningJobs.filter(!_.isCompleted).size)
    }
    
    runningJobs.foreach(tsk => {
      tsk.value.get match {
        case Failure(_) | Success(JobFailure()) => "task failed"
        case Success(JobSuccess()) => "task succeeded"
      }
    })

    server.close()
  }
}

object Task {
  def main(args: Array[String]) {
    val addr = InetAddress.getByName(args(0))
    val port = args(1).toInt

    val skt = new Socket(addr, port)
    val in = new ObjectInputStream(new DataInputStream(skt.getInputStream()))
    val out = new ObjectOutputStream(new DataOutputStream(skt.getOutputStream()))

    val operation = in.readObject().asInstanceOf[Unit=>Unit]
    operation()

    out.writeObject(JobSuccess())
  }
}

