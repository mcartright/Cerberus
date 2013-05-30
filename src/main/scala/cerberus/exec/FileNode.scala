package cerberus.exec

import cerberus.io._

import scala.reflect.ClassTag
import scala.collection.GenTraversableOnce
import scala.collection.mutable.ArrayBuilder
import scala.math.Ordering

/**
 * Output to multiple files using a simple round-robin dispatch
 */
class RoundRobinDistribNode[T :ClassTag](val paths: Set[String])(implicit val encoding: Protocol) extends Node[T] {
  var outputs: Array[Writer[T]] = null
  var nextOutput = 0
  def init(cfg: RuntimeConfig) {
    outputs = paths.map(encoding.getWriter[T](_)).toArray
  }
  def close() {
    outputs.foreach(_.close())
  }
  def process(next: T) {
    outputs(nextOutput).put(next)
    nextOutput = (nextOutput + 1) % outputs.size
  }
}

/**
 * Output to multiple files using a simple .hashCode % paths.size dispatch
 */
class HashDistribNode[T :ClassTag](val paths: Set[String])(implicit val encoding: Protocol) extends Node[T] {
  var outputs: Array[Writer[T]] = null
  def init(cfg: RuntimeConfig) {
    outputs = paths.map(encoding.getWriter[T](_)).toArray
  }
  def close() {
    outputs.foreach(_.close())
  }
  def process(next: T) {
    val destination = next.hashCode() % outputs.size
    outputs(destination).put(next)
  }
}

/**
 * Output to a single file, using the specified encoding
 */
class FileNode[T :ClassTag](val path: String)(implicit val encoding: Protocol) extends Node[T] {
  var output: Writer[T] = null
  
  def init(cfg: RuntimeConfig) {
    output = encoding.getWriter[T](path)
  }
  def process(next: T) {
    assert(output != null)
    output.put(next)
  }
  def close() {
    output.close()
  }
}

