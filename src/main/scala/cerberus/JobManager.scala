package cerberus

import cerberus.io._
import cerberus.exec._
import java.io.{Serializable => Encodable}
import java.io._
import scala.concurrent._
import scala.reflect.ClassTag
import ExecutionContext.Implicits.global
import scala.util.{Try, Success, Failure}
import java.net._

class JobParameters extends Encodable {
  val tmpDirectories = Set[String]("/tmp")
}

object JobSocket {
  def apply(address: String, port: Int) = {
    println("JobSocket.apply("+address+","+port+")")
    new JobSocket(new Socket(InetAddress.getByName(address), port))
  }
  def apply(skt: Socket) = new JobSocket(skt)
}

class JobSocket(val socket: Socket) {
  lazy val in = new ObjectInputStream(new DataInputStream(socket.getInputStream()))
  lazy val out = new ObjectOutputStream(new DataOutputStream(socket.getOutputStream()))

  // typed read, write, close
  def read[T <:Encodable :ClassTag]() = in.readObject().asInstanceOf[T]
  def write[T <:Encodable :ClassTag](obj:T) = out.writeObject(obj)
  def close() = socket.close()
}

abstract class AbstractJobStep(val inFile: String, val outFile: String) extends Encodable {
  def run(): Int
}

class JobStep[A <: Encodable :ClassTag, B <: Encodable :ClassTag](op: A=>B, inFile: String, outFile: String) extends
AbstractJobStep(inFile, outFile) {
  def run(): Int = {
    try {
      val in = JavaObjectProtocol().getReader[A](inFile)
      val out = JavaObjectProtocol().getWriter[B](outFile)
      
      in.foreach(a => {
        out.put(op(a))
      })
      
      out.close()
      return 0
    } catch {
      case _: Throwable => return -1
    }
  }
}

object JobTest {
  def main(args: Array[String]) {
    val server = new ServerSocket(0)
    val port = server.getLocalPort

    val qsub = new LocalJobService
    val localhost = InetAddress.getLocalHost.getCanonicalHostName

    {
      val wr = io.JavaObjectProtocol().getWriter[java.lang.Integer]("input.flow")
      (0 until 10).map(new java.lang.Integer(_)).foreach(wr.put)
      wr.close()
    }

    val jobId = qsub.spawnJob("cerberus.JobRunner", Array(localhost, port.toString))
    println("spawned "+jobId)

    val operation: (Int=>String) = _.toString
    val res = JobRunner.dispatch(server.accept(), new JobStep(operation, "input.flow", "output.flow"))
  }
}

object JobRunner {
  def dispatch(skt: Socket, task: AbstractJobStep): Future[Int] = {
    val client = JobSocket(skt)

    // begin protocol
    client.write(new JobParameters)
    client.write(task)

    future { 
      val result = client.read[java.lang.Integer]()
      client.close()
      result.intValue
    }
  }

  def main(args: Array[String]) {
    // TODO
    //System.setOut(new PrintStream(new FileOutputStream("stdout")));
    //System.setErr(new PrintStream(new FileOutputStream("stderr")));
    val server = JobSocket(args(0), args(1).toInt)

    // begin protocol
    val params = server.read[JobParameters]
    val task = server.read[AbstractJobStep]
    val res = new java.lang.Integer(task.run())
    server.write(res)
    server.close()
  }
}

