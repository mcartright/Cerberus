package cerberus

import java.io.{Serializable => Encodable}
import reflect.ClassTag

object Flow {
  def empty[T <: Encodable : ClassTag] = new Flow[T] {
    def hasNext = false
    def next = ???
  }
}

abstract class Flow[T <: Encodable : ClassTag] {
  def hasNext: Boolean
  def next: T
  def write(path: String) {
    var fp = FileFlow.openOutputStream(path)
    while(hasNext) {
      fp.writeObject(next)
    }
    fp.close()
  }
  def map[B <: Encodable : ClassTag](op: T=>B) = new MappedFlow(this, op)
  def flatMap[B <: Encodable : ClassTag](op: T=>Flow[B]) = new FlatMappedFlow(this, op)
  
  // bad idea, only for testing
  def toArray: Array[T] = {
    var bldr = Array.newBuilder[T]
    while(hasNext) {
      bldr += next
    }
    bldr.result
  }
  
  def splitrr(path: String, n: Int) = {
    val streamNames = (0 until n).map(idx => "%s_%d".format(path, idx))
    val streams = streamNames.map(fn => FileFlow.openOutputStream(fn))
    
    var curStream = 0

    while(hasNext) {
      streams(curStream).writeObject(next)
      curStream = (curStream + 1) % n
    }
    streams.foreach(_.close())

    streamNames
  }
}

class SeqFlow[T <: Encodable : ClassTag](val data: IndexedSeq[T]) extends Flow[T] {
  var i=0
  def hasNext = i < data.size
  def next = {
    require(i < data.size)
    val cur = data(i)
    i+=1
    cur
  }
}

object FileFlow {
  import java.io._
  val MagicNumber = 0xdeadbeef
  
  def openInputStream(path: String): ObjectInputStream = {
    var fp: ObjectInputStream = null
    try {
      fp = new ObjectInputStream(new DataInputStream(new FileInputStream(path)))
      
      if(fp.readInt != FileFlow.MagicNumber) {
        println("FileFlow("+path+") has a bad magic number!")
        ???
      }
      return fp
    } catch {
      case x : Throwable => {
        if(fp != null) fp.close()
        println("FileFlow("+path+") could not be read!")
        throw x
      }
    }
  }
  
  def openOutputStream(path: String): ObjectOutputStream = {
    var fp: ObjectOutputStream = null

    try {
      fp = new ObjectOutputStream(new DataOutputStream(new FileOutputStream(path)))
      
      fp.writeInt(MagicNumber)
      
      return fp
    } catch {
      case x : Throwable => {
        if(fp != null) fp.close()
        println("FileFlow("+path+") could not be opened for writing!")
        throw x
      }
    }
  }
}

class FileFlow[T <: Encodable : ClassTag](val inputPath: String) extends Flow[T] {
  var fp = FileFlow.openInputStream(inputPath)
  var current = tryNext

  def tryNext: Option[T] = try {
    Some(fp.readObject.asInstanceOf[T])
  } catch {
    case _: java.io.IOException | _: ClassCastException => None
  }

  def hasNext = current.nonEmpty
  def next = {
    val last = current.get
    current = tryNext
    last
  }
}

class MappedFlow[A <: Encodable : ClassTag, B <: Encodable: ClassTag](input: Flow[A], op: A=>B) extends Flow[B] {
  def hasNext = input.hasNext
  def next = op(input.next)
}

class FlatMappedFlow[A <: Encodable : ClassTag, B <: Encodable : ClassTag] (input: Flow[A], op: A=>Flow[B]) extends Flow[B] {
  var curFlow: Flow[B] = unflatNext
    
  def unflatNext: Flow[B] = {
    if(input.hasNext) {
      op(input.next)
    } else Flow.empty
  }
  def hasNext = {
    if(!curFlow.hasNext) {
      curFlow = unflatNext
    }
    curFlow.hasNext
  }
  def next = curFlow.next
}

