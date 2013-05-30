package cerberus

import cerberus.io._
import cerberus.service._
import cerberus.exec._

import java.io._
import scala.concurrent._
import scala.concurrent.duration._
import scala.reflect.ClassTag
import ExecutionContext.Implicits.global
import java.net._

trait AbstractJobStep extends Encodable {
  /** each job step needs to provide a unique id */
  def id: String
  /** client-side execution */
  def run(cfg: RuntimeConfig): Int
}

class ExecutorStep[T :ClassTag](
  val src: Source[T],
  val node: Node[T],
  val name: String
) extends AbstractJobStep {
  def id = name
  def run(cfg: RuntimeConfig): Int = {
    Executor(src, node).run(cfg)
    cfg.deleteAllTemporaries()
    0
  }
}

class JobDispatcher {
  val server = JServer()
  val qsub = new LocalJobService

  def run[T :ClassTag](
    src: Source[T], 
    node: Node[T],
    name: String
  )(implicit conf: SharedConfig): Future[Int] = {
    val jobId = qsub.spawnJob("cerberus.JobRunner", Array(server.hostName, server.port.toString))
    println("spawned "+jobId)

    JobRunner.dispatch(server.accept(), new ExecutorStep(src, node, name), conf)
  }

  def runSync[T :ClassTag](
    src: Source[T],
    node: Node[T],
    name: String
  )(implicit conf: SharedConfig): Unit = {
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
  def dispatch(client: JSocket, task: AbstractJobStep, conf: SharedConfig): Future[Int] = {
    // begin protocol
    client.write(new RuntimeConfig(task.id, conf))
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
    val server = JSocket(args(0), args(1).toInt)

    // begin protocol
    val cfg = server.read[RuntimeConfig]
    val task = server.read[AbstractJobStep]
    val res = new java.lang.Integer(task.run(cfg))
    server.write(res)
    server.close()
  }
}

