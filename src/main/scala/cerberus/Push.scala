/**
 * Push node execution
 */

package cerberus

import cerberus.io._
import scala.reflect.ClassTag
import scala.collection.GenTraversableOnce
import scala.math.Ordering

// TODO, make this configuration better
class RuntimeConfig(val jobUniq: String) {
  // for helping make files 
  var uid = 0
  
  def nextFileName() = {
    uid += 1
    Util.returnPath(jobUniq + "/file"+uid)
  }
  def nextScratchName() = {
    uid += 1
    Util.returnPath("/tmp/"+jobUniq+"/scratch"+uid)
  }
}

trait Node[T <:Encodable] {
  def conf(cfg: RuntimeConfig): Unit
  def process(next: T): Unit
  def flush(): Unit
}

/**
 * Output to multiple files using a simple round-robin dispatch
 */
class RoundRobinDistribNode[T <:Encodable](val paths: Set[String])(implicit val encoding: Protocol) extends Node[T] {
  @transient val outputs = paths.map(encoding.getWriter[T](_)).toArray
  @transient var nextOutput = 0
  def conf(cfg: RuntimeConfig) { }
  def flush() {
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
class HashDistribNode[T <:Encodable](val paths: Set[String])(implicit val encoding: Protocol) extends Node[T] {
  @transient val outputs = paths.map(encoding.getWriter[T](_)).toArray
  def conf(cfg: RuntimeConfig) { }
  def flush() {
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
class FileNode[T <:Encodable](val path: String)(implicit val encoding: Protocol) extends Node[T] {
  @transient val output = encoding.getWriter[T](path)
  def conf(cfg: RuntimeConfig) { }
  def process(next: T) {
    output.put(next)
  }
  def flush() {
    output.close()
  }
}

class MappedNode[A <:Encodable, B <:Encodable](val child: Node[B], oper: A=>B) extends Node[A] {
  def conf(cfg: RuntimeConfig) = child.conf(cfg)
  def process(next: A) = child.process(oper(next))
  def flush() = child.flush()
}

class FlatMappedNode[A <:Encodable, B<:Encodable](val child: Node[B], val oper: A=>GenTraversableOnce[B]) extends Node[A] {
  def conf(cfg: RuntimeConfig) = child.conf(cfg)
  def flush() = child.flush()
  
  def process(next: A) = {
    oper(next).foreach(child.process(_))
  }
}

class ForeachedNode[T <:Encodable, U](val oper: T=>U) extends Node[T] {
  def conf(cfg: RuntimeConfig) { }
  def flush() { }
  def process(next: T) {
    oper(next)
  }
}

class FilteredNode[T <:Encodable](val child: Node[T], oper: T=>Boolean) extends Node[T] {
  def conf(cfg: RuntimeConfig) = child.conf(cfg)
  def process(next: T) = if(oper(next)) { child.process(next) }
  def flush() = child.flush()
}

class MultiNode[T <:Encodable](val children: Seq[Node[T]]) extends Node[T] {
  def conf(cfg: RuntimeConfig) = children.foreach(_.conf(cfg))
  def process(next: T) = children.foreach(_.process(next))
  def flush() = children.foreach(_.flush())
}

class SortedNode[T <:Encodable :ClassTag](
  val child: Node[T],
  val bufferSize: Int=8192
)(implicit val ord: math.Ordering[T], implicit val encoding: Protocol) extends Node[T] {
  // keep up to bufferSize elements in memory at once
  @transient val buffer = new Array[T](bufferSize)
  // fill up diskBuffers with the list of files to merge later
  @transient var diskBuffers = Set[String]()
  // how many are in the buffer
  @transient var count = 0 

  // merge 10 files at a time
  @transient val MergeFileCount = 10

  // save this locally
  @transient var rcfg: RuntimeConfig = null
  def conf(cfg: RuntimeConfig) {
    rcfg = cfg
    child.conf(cfg)
  }

  def pushBufferToDisk() {
    val tmpName = rcfg.nextScratchName()

    // sort buffer
    // use Java's in-place sort
    // Scala's doesn't let you specify part of an array to sort
    java.util.Arrays.sort(buffer.asInstanceOf[Array[java.lang.Object]], 0, count, ord.asInstanceOf[java.util.Comparator[_ >: Any]])
    
    // put up to count things
    val fp = encoding.getWriter[T](tmpName)
    var idx =0
    while(idx < count) {
      fp.put(buffer(idx))
      idx += 1
    }
    fp.close()
    
    // keep this buffer
    diskBuffers += tmpName
    count = 0
  }

  def deleteFiles(names: Set[String]) {
    names.foreach(path => Util.deleteFile(path))
  }

  def process(next: T) {
    if(count == bufferSize) {
      pushBufferToDisk()
    }
    buffer(count) = next
    count += 1
  }

  def merge(bufNames: Set[String], out: Node[T]) {
    // turn each sorted buf into a BufferedIterator[T]
    val pullStreams: Set[Reader[T]] = bufNames.map(encoding.getReader[T](_)) 
    val pullIters: Set[BufferedIterator[T]] = pullStreams.map(_.buffered)

    while(pullIters.exists(_.hasNext)) {
      // find the minimum of all the flows and return that
      val minIter: BufferedIterator[T] = pullIters.filter(_.hasNext).minBy(_.head)
      out.process(minIter.next)
    }
    out.flush()
    
    pullStreams.foreach(_.close())
    deleteFiles(bufNames)
  }

  def flush() {
    pushBufferToDisk()

    var buffersToMerge: Set[String] = diskBuffers

    while(buffersToMerge.size > MergeFileCount) {
      buffersToMerge = buffersToMerge.grouped(MergeFileCount).map(fgrp => {
        val scratchFile = rcfg.nextScratchName()
        merge(fgrp.toSet, new FileNode[T](scratchFile))
        scratchFile
      }).toSet
    }

    // put the final set to the child
    merge(buffersToMerge, child)
  }
}


