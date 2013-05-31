/**
 * Push node execution
 */

package cerberus.exec

import cerberus.io._

import scala.reflect.ClassTag
import scala.collection.GenTraversableOnce
import scala.collection.mutable.ArrayBuilder
import scala.math.Ordering

/**
 *
 * The Node is an object that exists at build (dispatch) time
 * and at run (compute) time. 
 *
 * The constructor is called at dispatch time. Use this to store
 * any configuration that you need - such as file names.
 *
 * init() is called at run time, use this to open any state you need
 * namely, open files for reading or writing
 *
 * process(T) is called for each element the node must process
 *
 * close() is called when all the elements have been seen
 *
 * mark data set up via init() as ``var foo: Type = null``
 * this way, you'll get a null error if you forget to initialize them
 *
 * note that ``@transient lazy val``s would also work, bu we decided on the
 * explicit flow of init -> process -> process... -> close
 *
 */
abstract class Node[T] extends Encodable {
  def init(cfg: RuntimeConfig): Unit
  def process(next: T): Unit
  def close(): Unit
}

/**
 * EchoNode -- for debugging
 */
class EchoNode[T](val id: String, val child: Node[T]) extends Node[T] {
  def init(cfg: RuntimeConfig) {
    println("EchoNode "+id+" init")
    child.init(cfg)
  }
  def close() {
    println("EchoNode "+id+" close")
    child.close()
  }
  def process(next: T) {
    println("EchoNode "+id+" process "+next)
    child.process(next)
  }
}

/**
 * NullNode -- for debugging
 */
class NullNode[T] extends Node[T] {
  def init(cfg: RuntimeConfig) { }
  def process(next: T) { }
  def close() { }
}

class MappedNode[A, B](val child: Node[B], val oper: A=>B) extends Node[A] {
  def init(cfg: RuntimeConfig) {
    TryInitAndClose.init(oper)
    child.init(cfg)
  }
  def process(next: A) = child.process(oper(next))
  def close() {
    TryInitAndClose.close(oper)
    child.close()
  }
}

class FlatMappedNode[A, B](val child: Node[B], val oper: A=>GenTraversableOnce[B]) extends Node[A] {
  def init(cfg: RuntimeConfig) {
    TryInitAndClose.init(oper)
    child.init(cfg)
  }
  def close() {
    TryInitAndClose.close(oper)
    child.close()
  }
  
  def process(next: A) = {
    oper(next).foreach(child.process(_))
  }
}

class ForeachedNode[T, U](val oper: T=>U) extends Node[T] {
  def init(cfg: RuntimeConfig) {
    TryInitAndClose.init(oper)
  }
  def close() {
    TryInitAndClose.close(oper)
  }
  def process(next: T) {
    oper(next)
  }
}

class FilteredNode[T](val child: Node[T], val oper: T=>Boolean) extends Node[T] {
  def init(cfg: RuntimeConfig) {
    TryInitAndClose.init(oper)
    child.init(cfg)
  }
  def process(next: T) = if(oper(next)) { child.process(next) }
  def close() {
    TryInitAndClose.close(oper)
    child.close()
  }
}

class MultiNode[T](val children: Seq[Node[T]]) extends Node[T] {
  def init(cfg: RuntimeConfig) = children.foreach(_.init(cfg))
  def process(next: T) = children.foreach(_.process(next))
  def close() = children.foreach(_.close())
}

class SortedNode[T :ClassTag](
  val child: Node[T],
  val bufferSize: Int=8192
)(implicit val ord: math.Ordering[T], implicit val encoding: Protocol) extends Node[T] {
  // constant: merge 10 files at a time
  val MergeFileCount = 10

  // keep up to bufferSize elements in memory at once
  var buffer: ArrayBuilder[T] = null
  // save this locally
  var rcfg: RuntimeConfig = null
  // fill up diskBuffers with the list of files to merge later
  var diskBuffers = Set[String]()
  // how many are in the buffer
  var count = 0 
  // how many have passed through this node
  var totalCount = 0

  def init(cfg: RuntimeConfig) {
    // setup up member variables
    rcfg = cfg

    //use an arraybuilder as a mutable array
    buffer = ArrayBuilder.make()
    buffer.sizeHint(bufferSize)
    
    assert(buffer != null)

    // init my children
    child.init(cfg)
  }

  def pushBufferToDisk() {
    assert(count != 0)

    val tmpName = rcfg.nextScratchName()

    //println("SortedNode["+buffer(0).getClass.getName+"] pushBufferToDisk "+count)

    // sort buffer
    val sbuf = buffer.result().sorted
    
    // put up to count things
    val fp = encoding.getWriter[T](tmpName)
    var idx = 0
    while(idx < count) {
      fp.put(sbuf(idx))
      idx += 1
    }
    fp.close()
    
    // keep this buffer
    diskBuffers += tmpName
    
    // clear buffer
    buffer.clear()
    count = 0
  }

  def deleteFiles(names: Set[String]) {
    names.foreach(path => Util.deleteFile(path))
  }

  def process(next: T) {
    if(count == bufferSize) {
      pushBufferToDisk()
    }
    buffer += next
    count += 1
    totalCount += 1
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
    out.close()
    
    pullStreams.foreach(_.close())
    deleteFiles(bufNames)
  }

  // this calls process on everything still stuck in its buffer,
  // and then close on its child
  def close() {
    if(count == 0) {
      child.close()
      return
    }
    pushBufferToDisk()

    var buffersToMerge: Set[String] = diskBuffers

    while(buffersToMerge.size > MergeFileCount) {
      buffersToMerge = buffersToMerge.grouped(MergeFileCount).map(fgrp => {
        val scratchFile = rcfg.nextScratchName()
        
        // create a fileNode for flushing
        val tmpFileNode = new ScratchFileNode[T](scratchFile)
        tmpFileNode.init(rcfg)
        merge(fgrp.toSet, tmpFileNode)

        scratchFile
      }).toSet
    }

    // put the final set to the child
    merge(buffersToMerge, child)
  }
}


