package cerberus

package object io {
  import java.io._
  type Encodable = java.io.Serializable
  type BinIStream = java.io.DataInputStream
  type BinOStream = java.io.DataOutputStream

  // TODO streamcreator gzipping?
  def inputStream(path: String) = {
    new BinIStream(new FileInputStream(path))
  }
  def outputStream(path: String) = {
    new BinOStream(new FileOutputStream(path))
  }

  trait Reader[T <:Encodable] extends Iterator[T] {
    def hasNext: Boolean
    def next(): T
    def close(): Unit
  }
  trait Writer[T <:Encodable] {
    def put(obj: T)
    def close(): Unit
  }

  /**
   *
   * @see JavaObjectProtocol
   */
  trait Protocol {
    // abstract
    def getReader[T <:Encodable](is: BinIStream): Reader[T]
    def getWriter[T <:Encodable](os: BinOStream): Writer[T]
    
    // concrete
    def getReader[T <:Encodable](path: String): Reader[T] = getReader(inputStream(path))
    def getWriter[T <:Encodable](path: String): Writer[T] = getWriter(outputStream(path))
  }
  // TODO ThriftObjectProtocol()

  /**
   * A DataFile is a path and an encoding
   */
  case class DataFile[T <:Encodable](path: String, encoding: Protocol=JavaObjectProtocol()) {
    def open = encoding.getReader[T](path)
  }

}

