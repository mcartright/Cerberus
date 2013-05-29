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

  val myTempFolder = Util.mkdir("/tmp/"+jobUniq)
  println("mkdir "+myTempFolder)
  
  def nextScratchName() = {
    uid += 1
    Util.generatePath(myTempFolder+"/scratch"+uid)
  }

  def deleteAllTemporaries() {
    //Util.delete(myTempFolder)
  }
}

/**
 * utility methods that duck types using reflection;
 *
 * calling init and close if they're available on function objects
 */
object TryInitAndClose {
  def init(duck: Any) {
    val methods = duck.getClass.getMethods.filter { m => 
      m.getName.contains("init") &&
      m.getParameterTypes.size == 0 &&
      m.getTypeParameters.size == 0
    }

    if(methods.nonEmpty) methods.head.invoke(duck)
  }
  def close(duck: Any) {
    val methods = duck.getClass.getMethods.filter { m => 
      m.getName == "close" &&
      m.getParameterTypes.size == 0 &&
      m.getTypeParameters.size == 0
    }

    if(methods.nonEmpty) methods.head.invoke(duck)
  }
}

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
trait Node[T <:Encodable] extends Encodable {
  def init(cfg: RuntimeConfig): Unit
  def process(next: T): Unit
  def close(): Unit
}

/**
 * Output to multiple files using a simple round-robin dispatch
 */
class RoundRobinDistribNode[T <:Encodable](val paths: Set[String])(implicit val encoding: Protocol) extends Node[T] {
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
class HashDistribNode[T <:Encodable](val paths: Set[String])(implicit val encoding: Protocol) extends Node[T] {
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
class FileNode[T <:Encodable](val path: String)(implicit val encoding: Protocol) extends Node[T] {
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

class MappedNode[A <:Encodable, B <:Encodable](val child: Node[B], val oper: A=>B) extends Node[A] {
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

class FlatMappedNode[A <:Encodable, B<:Encodable](val child: Node[B], val oper: A=>GenTraversableOnce[B]) extends Node[A] {
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

class ForeachedNode[T <:Encodable, U](val oper: T=>U) extends Node[T] {
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

class FilteredNode[T <:Encodable](val child: Node[T], val oper: T=>Boolean) extends Node[T] {
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

class MultiNode[T <:Encodable](val children: Seq[Node[T]]) extends Node[T] {
  def init(cfg: RuntimeConfig) = children.foreach(_.init(cfg))
  def process(next: T) = children.foreach(_.process(next))
  def close() = children.foreach(_.close())
}

class SortedNode[T <:Encodable :ClassTag](
  val child: Node[T],
  val bufferSize: Int=8192
)(implicit val ord: math.Ordering[T], implicit val encoding: Protocol) extends Node[T] {
  // constant: merge 10 files at a time
  val MergeFileCount = 10

  // keep up to bufferSize elements in memory at once
  var buffer: Array[T] = null
  // save this locally
  var rcfg: RuntimeConfig = null
  // fill up diskBuffers with the list of files to merge later
  var diskBuffers = Set[String]()
  // how many are in the buffer
  var count = 0 

  def init(cfg: RuntimeConfig) {
    // setup up member variables
    rcfg = cfg
    buffer = new Array[T](bufferSize)
    assert(buffer != null)

    // init my children
    child.init(cfg)
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
    out.close()
    
    pullStreams.foreach(_.close())
    deleteFiles(bufNames)
  }

  // this calls process on everything still stuck in its buffer,
  // and then close on its child
  def close() {
    pushBufferToDisk()

    var buffersToMerge: Set[String] = diskBuffers

    while(buffersToMerge.size > MergeFileCount) {
      buffersToMerge = buffersToMerge.grouped(MergeFileCount).map(fgrp => {
        val scratchFile = rcfg.nextScratchName()
        
        // create a fileNode for flushing
        val tmpFileNode = new FileNode[T](scratchFile)
        tmpFileNode.init(rcfg)
        merge(fgrp.toSet, tmpFileNode)

        scratchFile
      }).toSet
    }

    // put the final set to the child
    merge(buffersToMerge, child)
  }
}


