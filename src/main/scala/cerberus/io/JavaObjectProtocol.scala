package cerberus.io

import java.io.{ObjectOutputStream, ObjectInputStream}

case class JavaObjectProtocol() extends Protocol {
  def getReader[T <:Encodable](is: BinIStream) = new JavaObjectReader(is)
  def getWriter[T <:Encodable](os: BinOStream) = new JavaObjectWriter(os)
  //TODO @specialized for primitives
}

class JavaObjectReader[T <:Encodable](is: BinIStream) extends Reader[T] {
  val fp = new ObjectInputStream(is)

  // current is an option type that goes to None at the end of the valid stream
  var current = tryNext
  private def tryNext: Option[T] = try {
    Some(fp.readObject.asInstanceOf[T])
  } catch {
    case _: java.io.IOException => None
    case cce: ClassCastException => throw cce
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

class JavaObjectWriter[T <:Encodable](is: BinOStream) extends Writer[T] {
  val fp = new ObjectOutputStream(is)
  def put(obj: T) = fp.writeObject(obj)
  def close() = fp.close()
}

