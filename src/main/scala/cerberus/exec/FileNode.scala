package cerberus.exec

import cerberus.io._

import scala.reflect.ClassTag
import scala.collection.GenTraversableOnce
import scala.collection.mutable.ArrayBuilder
import scala.math.Ordering

/** 
 * Writers
 *
 */
class SplitOutputNode[T :ClassTag](val path: String)(implicit val encoding: Protocol) extends Node[T] {
  var writer: SplitWriter[T] = null
  
  def init(cfg: RuntimeConfig) {
    assert(cfg.isSplitJob) // TODO move this to compile time
    writer = new SplitWriter(path, cfg.nodeId, cfg.numNodes)
  }
  def process(next: T) = writer.put(next)
  def close() = writer.close()
}

class RoundRobinDistribOutputNode[T :ClassTag](val path: String)(implicit val encoding: Protocol) extends Node[T] {
  var writers: IndexedSeq[SplitWriter[T]] = null
  var nextWriter: Int = 0

  def init(cfg: RuntimeConfig) {
    writers = (0 until cfg.numNodes).map(id => new SplitWriter(path, id, cfg.numNodes))
  }
  def process(next: T) {
    writers(nextWriter).put(next)
    nextWriter = (nextWriter+1) % writers.size
  }
  def close() {
    writers.foreach(_.close())
  }
}

class ScratchFileNode[T :ClassTag](val path: String)(implicit val encoding: Protocol) extends Node[T] {
  var writer: Writer[T] = null
  def init(cfg: RuntimeConfig) {
    writer = encoding.getWriter[T](path)
  }
  def process(next: T) = writer.put(next)
  def close() = writer.close()
}

//TODO HashedDistribOutputNode

