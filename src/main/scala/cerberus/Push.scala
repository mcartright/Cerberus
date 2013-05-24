/**
 * Push node execution
 */

package cerberus

import cerberus.io._

trait Node[T <:Encodable] {
  def process(next: T)
  def flush() { }
}

class FileNode[T <:Encodable](val path: String, val encoding: Protocol) extends Node[T] {
  val output = encoding.getWriter[T]
  def process(next: T) {
    output.put(next)
  }
  def flush() {
    output.flush()
    output.close()
  }
}

class MappedNode[A <:Encodable, B <:Encodable](val child: Node[B], oper: A=>B) extends Node[A] {
  def process(next: A) = child.process(oper(next))
}

class FilteredNode[T <:Encodable](val child: Node[T], oper: T=>Boolean) extends Node[T] {
  def process(next: A) = if(oper(next)) { child.process(next) }
}

class MultiNode[T <:Encodable](val children: Seq[Node[T]]) extends Node[T] {
  def process(next: T) = children.foreach(_.process(next))
}

class SortedNode[T <:Encodable](val child: Node[T], val encoding: Protocol, val bufferSize: Int=8192)(implicit ord: math.Ordering[T]) {
  // keep up to bufferSize elements in memory at once
  val buffer = new Array[T](bufferSize)
  // fill up diskBuffers with the list of files to merge later
  var diskBuffers = Set[String]()
  // how many are in the buffer
  var count = 0 

  // only one sort on this computer at a time :(
  def nextTempName() = {
    "/tmp/sortbuf"+diskBuffers.size
  }

  def pushBufferToDisk() {
    val tmpName = nextTempName()
    
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

  def process(next: T) {
    if(count == bufferSize) {
      pushBufferToDisk()
    }
    buffer(count) = next
    count += 1
  }
  def flush() {
    pushBufferToDisk()
    
    // turn each diskBuffer into a sorted, BufferedIterator[T]
    val pullStreams = diskBuffers.foreach(encoding.getReader).buffered

    while(pullStreams.exists(_.hasNext)) {
      // find the minimum of all the flows and return that
      var bestIdx = -1
      var bestVal: T = inputs.find(_.hasNext).get.head
      
      (0 until inputs.size).foreach(idx => {
        if(inputs(idx).hasNext) {
          if(bestIdx == -1 || ord.lt(inputs(idx).head, bestVal)) {
            bestIdx = idx
            bestVal = inputs(idx).head
          }
        }
      })

      child.process(bestVal)
      inputs(bestIdx).next
    }
  }
}


