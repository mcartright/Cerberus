package cerberus.exec

/**
 * There are only a handful of Pull nodes (Source) in the execution graph, but they hand off to a Push node (Node)
 */

import cerberus.io._
import scala.reflect.ClassTag
import scala.collection.GenTraversableOnce

/**
 * This class is responsible for execution of a source and a graph
 */
case class Executor[T :ClassTag](val src: Source[T], val pushTo: Node[T]) {
  def run(cfg: RuntimeConfig) {
    assert(cfg != null)
    
    // init runtime configuration of the graph
    pushTo.init(cfg)
    
    // process all the data coming from the source
    val iter: Reader[T] = src.getReader()
    while(iter.hasNext) {
      pushTo.process(iter.next())
    }
    //iter.foreach(pushTo.process(_))
    iter.close()

    // close out any buffered steps
    pushTo.close()
  }
}

/**
 * Generic interface to a Source
 * A Reader[T] is nothing more than a closeable Iterator[T]
 */
abstract class Source[T :ClassTag] extends Encodable {
  def getReader(): Reader[T]
}

case class FileSource[T :ClassTag](val path: String)(implicit val encoding: Protocol) extends Source[T] {
  def getReader(): Reader[T] = {
    encoding.getReader[T](path)
  }
}

case class MergedFileSource[T :ClassTag](val paths: Set[String])(implicit val encoding: Protocol) extends Source[T] {
  assert(paths.size != 0)
  def getReader() = new Reader[T] {
    val orderedFiles = paths.toIndexedSeq
    var fp = encoding.getReader[T](orderedFiles(0))
    var i = 1

    def hasNext: Boolean = {
      // if this one is done and there's another
      while(!fp.hasNext && i < paths.size) {
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
  val paths: Set[String]
)(
  implicit val ord: math.Ordering[T],
  implicit val encoding: Protocol
) extends Source[T] {
  assert(paths.size != 0)
  def getReader() = new Reader[T] {
    // keep the files around for closing
    val files = paths.map(encoding.getReader[T](_))
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

case class TraversableSource[T :ClassTag](seq: GenTraversableOnce[T]) extends Source[T] {
  val data = seq.toIndexedSeq
  val len = data.size
  def getReader = {
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


