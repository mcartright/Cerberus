package cerberus.exec

/**
 * There are only a handful of Pull nodes (Source) in the execution graph, but they hand off to a Push node (Node)
 */

import cerberus.io._
import scala.reflect.ClassTag
import scala.collection.GenTraversableOnce

/**
 * Generic interface to a Source
 * A Reader[T] is nothing more than a closeable Iterator[T]
 */
abstract class Source[T :ClassTag] extends Encodable {
  def canDistrib: Boolean
  def getReader(cfg: RuntimeConfig): Reader[T]
}

case class SplitInputSource[T :ClassTag](val path: String)(implicit val encoding: Protocol) extends Source[T] {
  def canDistrib = true
  def getReader(cfg: RuntimeConfig): Reader[T] =
    new SplitReader[T](path, cfg.nodeId, cfg.numNodes)
}

case class MergedFileSource[T :ClassTag](val path: String)(implicit val encoding: Protocol) extends Source[T] {
  def canDistrib = false
  def getReader(cfg: RuntimeConfig) = new Reader[T] {
    val orderedFiles = (0 until cfg.numNodes).map(idx => SplitFormat.dataPath(path, idx))
    var fp = encoding.getReader[T](orderedFiles(0))
    var i = 1

    def hasNext: Boolean = {
      // if this one is done and there's another
      while(!fp.hasNext && i < orderedFiles.size) {
        fp.close() // close the current
        fp = encoding.getReader[T](orderedFiles(i)) // open the next
        i+=1
      }
      fp.hasNext
    }
    def next(): T = fp.next()
    def close() {
      fp.close()
    }
  }
}

case class SortedMergeSource[T :ClassTag](
  val path: String
)(
  implicit val ord: math.Ordering[T],
  implicit val encoding: Protocol
) extends Source[T] {
  def canDistrib = false
  def getReader(cfg: RuntimeConfig) = {
    new Reader[T] {
      // keep the files around for closing
      val files = SplitFormat.allReaders(path)
      // grab buffered iterators to perform the sorted merge
      val iters = files.map(_.buffered)

      def hasNext: Boolean = iters.exists(_.hasNext)
      def next(): T = {
        // return the minimum item
        iters.filter(_.hasNext).minBy(_.head).next()
      }
      def close() {
        files.foreach(_.close())
      }
    }
  }
}

case class TraversableSource[T :ClassTag](seq: GenTraversableOnce[T]) extends Source[T] {
  val data = seq.toIndexedSeq
  val len = data.size
  def canDistrib = false
  def getReader(cfg: RuntimeConfig) = {
    assert(!cfg.isSplitJob)
    new Reader[T] {
      var i = 0
      def hasNext = i < len
      def next() = {
        val obj = data(i)
        i += 1
        obj
      }
      def close() { }
    }
  }
}

