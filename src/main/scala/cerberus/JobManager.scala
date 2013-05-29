package cerberus

import cerberus.io._
import cerberus.exec._
import java.io.{Serializable => Encodable}
import java.io._
import scala.concurrent._
import scala.concurrent.duration._
import scala.reflect.ClassTag
import ExecutionContext.Implicits.global
import scala.util.{Try, Success, Failure}
import java.net._

class JobParameters extends Encodable {
  val tmpDirectories = Set[String]("/tmp")
}

object JobSocket {
  def apply(address: String, port: Int) = {
    new JobSocket(new Socket(InetAddress.getByName(address), port))
  }
  def apply(skt: Socket) = new JobSocket(skt)
}

class JobSocket(val socket: Socket) {
  // the laziness prevents a deadlock by both endpoints open the same kind of stream at the same time
  lazy val in = new ObjectInputStream(new DataInputStream(socket.getInputStream()))
  lazy val out = new ObjectOutputStream(new DataOutputStream(socket.getOutputStream()))

  // typed read, write, close
  def read[T <:Encodable :ClassTag]() = in.readObject().asInstanceOf[T]
  def write[T <:Encodable :ClassTag](obj:T) = out.writeObject(obj)
  def close() = socket.close()
}

trait AbstractJobStep extends Encodable {
  def run(jp: JobParameters): Int
}

class JobStep[A <: Encodable :ClassTag, B <: Encodable :ClassTag](op: A=>B, val inFile: String, val outFile: String) extends
AbstractJobStep {
  def run(jp: JobParameters): Int = {
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

class ExecutorStep[T <:Encodable](
  val src: Source[T],
  val node: Node[T],
  val name: String
) extends AbstractJobStep {
  def run(jp: JobParameters): Int = {
    val cfg = new RuntimeConfig(name)
    Executor(src, node).run(cfg)
    System.out.flush()
    System.err.flush()
    cfg.deleteAllTemporaries()
    0
  }
}

class JobDispatcher {
  val server = new ServerSocket(0)
  val port = server.getLocalPort

  val qsub = new LocalJobService
  val localhost = InetAddress.getLocalHost.getCanonicalHostName

  def run[T <:Encodable](src: Source[T], node: Node[T], name: String): Future[Int] = {
    val jobId = qsub.spawnJob("cerberus.JobRunner", Array(localhost, port.toString))
    println("spawned "+jobId)

    JobRunner.dispatch(server.accept(), new ExecutorStep(src, node, name))
  }

  def runSync[T <:Encodable](src: Source[T], node: Node[T], name: String): Unit = {
    val handle = run(src, node, name)
    while(!handle.isCompleted) {
      Time.snooze(30)
    }
    
    if(Await.result(handle, 100.millis) != 0) {
      throw new RuntimeException("Job Step Failed")
    }
  }

  def awaitMany(jobs: Set[Future[Int]]) {
    while(jobs.exists(!_.isCompleted)) {
      Time.snooze(30)
    }
    jobs.foreach { handle =>
      if(Await.result(handle, 100.millis) != 0) {
        throw new RuntimeException("Job Step Failed")
      }
    }
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
    // until we get to the job itself, append to the stdout, stderr files
    System.setOut(new PrintStream(new FileOutputStream("stdout",true)));
    System.setErr(new PrintStream(new FileOutputStream("stderr",true)));
    val server = JobSocket(args(0), args(1).toInt)

    // begin protocol
    val params = server.read[JobParameters]
    val task = server.read[AbstractJobStep]
    val res = new java.lang.Integer(task.run(params))
    server.write(res)
    server.close()
  }
}

