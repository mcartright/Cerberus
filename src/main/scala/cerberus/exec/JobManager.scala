package cerberus.exec

import cerberus.io._
import cerberus.service._

import scala.concurrent._
import scala.concurrent.duration._
import scala.reflect.ClassTag
import ExecutionContext.Implicits.global

trait AbstractJobStep extends Encodable {
  /** each job step needs to provide a unique id */
  def id: String
  /** client-side execution */
  def run(cfg: RuntimeConfig): Int
}

/**
 * This class is responsible for execution of a source and a graph
 */
case class Executor[T :ClassTag](val src: Source[T], val pushTo: Node[T]) {
  def run(cfg: RuntimeConfig) {
    assert(cfg != null)
    
    // init runtime configuration of the graph
    pushTo.init(cfg)
    
    // process all the data coming from the source
    val iter: Reader[T] = src.getReader(cfg)
    while(iter.hasNext) {
      pushTo.process(iter.next())
    }
    //iter.foreach(pushTo.process(_))
    iter.close()

    // close out any buffered steps
    pushTo.close()
  }
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

  private def spawn[T :ClassTag](
    src: Source[T], 
    node: Node[T],
    name: String,
    nodeId: Int,
    split: Boolean
  )(implicit conf: SharedConfig): Future[Int] = {
    assert(Class.forName(JobRunner.FullName) != null)
    val jobId = qsub.spawnJob(JobRunner.FullName, Array(server.hostName, server.port.toString))
    println("spawned "+jobId)

    JobRunner.dispatch(server.accept(), new ExecutorStep(src, node, name), nodeId, split, conf)
  }

  /**
   * Runs a parallel task on conf.distrib nodes
   * @see SharedConfig
   */
  def runDistributed[T :ClassTag](
    src: Source[T],
    node: Node[T],
    name: String
  )(implicit conf: SharedConfig) {
    assert(src.canDistrib)
    val futures = (0 until conf.distrib).map(id => {
      spawn(src, node, name, id, true)
    })
    
    while(futures.exists(!_.isCompleted)) {
      Time.snooze(30)
    }
    
    futures.foreach { handle =>
      if(Await.result(handle, 100.millis) != 0) {
        throw new RuntimeException("Job Step Failed")
      }
    }
  }

  /**
   * Runs a serial task on another node
   */
  def run[T :ClassTag](
    src: Source[T],
    node: Node[T],
    name: String
  )(implicit conf: SharedConfig) {
    val handle = spawn(src, node, name, 0, false)
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
  val FullName = "cerberus.exec.JobRunner"
  def dispatch(client: JSocket, task: AbstractJobStep, nodeId: Int, split: Boolean, conf: SharedConfig): Future[Int] = {
    // begin protocol
    client.write(new RuntimeConfig(task.id, conf, nodeId, split))
    client.write(task)

    future { 
      val result = client.read[java.lang.Integer]()
      client.close()
      result.intValue
    }
  }

  import java.io._
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

