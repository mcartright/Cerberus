package cerberus

import java.io.{Serializable => Encodable}
import reflect.ClassTag

object Flow {
  var uid = 0
  def gensym(txt: String) = {
    uid += 1
    txt + uid.toString
  }
  def empty[T <: Encodable : ClassTag] = new Flow[T] {
    def hasNext = false
    def next() = ???
  }
}

abstract class Flow[T <: Encodable : ClassTag] extends Encodable {
  def hasNext: Boolean
  def next(): T
  def write(path: String) {
    var fp = FileFlow.openOutputStream(path)
    while(hasNext) {
      fp.writeObject(next())
    }
    fp.close()
  }

  def buffered: BufferedFlow[T] = {
    val self = this
    new BufferedFlow[T] {
      private var hd: T = _
      private var hdDefined: Boolean = false

      def head: T = {
        if (!hdDefined) {
          hd = next()
          hdDefined = true
        }
        hd
      }

      def hasNext =
        hdDefined || self.hasNext

      def next() = {
        if (hdDefined) {
          hdDefined = false
          hd
        } else self.next()
      }
    }
  }

  def map[B <: Encodable : ClassTag](op: T=>B) = new MappedFlow(this, op)

  def flatMap[B <: Encodable : ClassTag](op: T=>Flow[B]) =
    new FlatMappedFlow(this, op)

  // forces evaluation to sort this in memory
  def inMemorySorted(implicit ord : math.Ordering[T]) =
    new SeqFlow(toArray.sorted)
  
  def sorted(implicit ord : math.Ordering[T]) =
    new SortedFlow(this)

  // bad idea, only for testing
  def toArray: Array[T] = {
    var bldr = Array.newBuilder[T]
    while(hasNext) {
      bldr += next()
    }
    bldr.result
  }

  def splitrr(path: String, n: Int) = {
    val streamNames = (0 until n).map(idx => "%s_%d".format(path, idx))
    val streams = streamNames.map(fn => FileFlow.openOutputStream(fn))

    var curStream = 0

    while(hasNext) {
      streams(curStream).writeObject(next())
      curStream = (curStream + 1) % n
    }
    streams.foreach(_.close())

    streamNames
  }
}

abstract class BufferedFlow[T <: Encodable : ClassTag] extends Flow[T] {
  def head: T
  def hasNext: Boolean
  def next(): T
}


class SeqFlow[T <: Encodable : ClassTag](val data: IndexedSeq[T])
    extends Flow[T] {
  var i=0
  def hasNext = i < data.size
  def next() = {
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
      fp =
        new ObjectOutputStream(new DataOutputStream(new FileOutputStream(path)))

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

class FileFlow[T <: Encodable : ClassTag](val inputPath: String)
    extends Flow[T] {
  var fp = FileFlow.openInputStream(inputPath)
  var current = tryNext

  def tryNext: Option[T] = try {
    Some(fp.readObject.asInstanceOf[T])
  } catch {
    case _: java.io.IOException | _: ClassCastException => None
  }

  def hasNext = current.nonEmpty
  def next() = {
    val last = current.get
    current = tryNext
    last
  }
}

class MappedFlow[A <: Encodable : ClassTag, B <: Encodable: ClassTag](
  input: Flow[A],
  op: A=>B
)
    extends Flow[B] {
  def hasNext = input.hasNext
  def next() = op(input.next())
}

class FlatMappedFlow[A <: Encodable : ClassTag, B <: Encodable : ClassTag](
  input: Flow[A],
  op: A=>Flow[B]
)
    extends Flow[B] {
  var curFlow: Flow[B] = unflatNext

  def unflatNext: Flow[B] = {
    if(input.hasNext) {
      op(input.next())
    } else Flow.empty
  }
  def hasNext = {
    if(!curFlow.hasNext) {
      curFlow = unflatNext
    }
    curFlow.hasNext
  }
  def next() = curFlow.next()
}

class RoundRobinReduceFlow[T <:Encodable :ClassTag](
  input: IndexedSeq[Flow[T]]
)
    extends Flow[T] {
  var i = 0
  def hasNext = input.exists(_.hasNext)
  def next() = {
    assert(hasNext)
    // find the next non-empty Flow
    while(!input(i).hasNext) {
      i = (i+1) % input.size
    }
    val result = input(i).next
    i = (i+1) % input.size
    result
  }
}

class SortedReduceFlow[T <:Encodable :ClassTag](
  rawInputs: IndexedSeq[Flow[T]]
)(
  implicit ord: math.Ordering[T]
)
    extends Flow[T] {
  val inputs = rawInputs.map(_.buffered)
  def hasNext = inputs.exists(_.hasNext)
  def next() = {
    assert(hasNext)
    
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

    inputs(bestIdx).next
  }
}


class SortedFlow[T <:Encodable :ClassTag](
  input: Flow[T],
  bufferSize: Int = 8192
)(
  implicit ord: math.Ordering[T]
  )
    extends Flow[T] {
  // keep up to bufferSize elements in memory at once
  var buffer = new Array[T](bufferSize)
  // fill up diskBuffers with the list of files to merge later
  var diskBuffers = Set[FileFlow[T]]()
  
  // evaluate immediately and flush to the buffers, sorting as we go
  while(input.hasNext) {
    var bufPtr = 0
    while(bufPtr < bufferSize && input.hasNext) {
      buffer(bufPtr) = input.next()
      bufPtr += 1
    }
    val tmpName = Flow.gensym("/tmp/sortbuf")
    new SeqFlow(buffer.take(bufPtr).sorted(ord)).write(tmpName)

    diskBuffers += new FileFlow[T](tmpName)
  }

  // todo heuristic for number of open files
  // until then, just open all the diskBuffers
  val reducer = new SortedReduceFlow[T](diskBuffers.toIndexedSeq)

  def hasNext = reducer.hasNext
  def next() = reducer.next()
}
