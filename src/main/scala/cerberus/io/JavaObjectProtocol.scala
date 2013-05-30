package cerberus.io

import scala.reflect.ClassTag
import java.io._

case class JavaObjectProtocol() extends Protocol {
  def getReader[T :ClassTag](is: BinIStream): Reader[T] = {
    val tag = implicitly[ClassTag[T]]
    tag.runtimeClass match {
      case _ => new JavaObjectReader[T](is)
    }
  }
  def getWriter[T :ClassTag](os: BinOStream) = {
    val tag = implicitly[ClassTag[T]]
    tag.runtimeClass match {
      case _ => new JavaObjectWriter[T](os)
    }
  }
}

class JavaObjectReader[T](is: BinIStream) extends Reader[T] {
  val fp = new ObjectInputStream(is)

  // current is an option type that goes to None at the end of the valid stream
  var current = tryNext
  private def tryNext: Option[T] = try {
    val nextVal = fp.readObject.asInstanceOf[T]
    //println("JavaObjectReader.read "+nextVal)
    Some(nextVal)
  } catch {
    case _: java.io.EOFException => None
  }
  
  def hasNext = current.nonEmpty
  def next() = {
    val last = current.get
    current = tryNext
    last
  }
  def close() {
    current = None
    fp.close()
  }
}

class JavaObjectWriter[T](is: BinOStream) extends Writer[T] {
  val fp = new ObjectOutputStream(is)
  def put(obj: T) = {
    //println("JavaObjectWriter.put "+obj)
    fp.writeObject(obj)
  }
  def close() = fp.close()
}

