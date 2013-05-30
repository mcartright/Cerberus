package cerberus

package object io {
  import java.io._
  import scala.reflect.ClassTag
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

  trait Reader[T] extends Iterator[T] {
    def hasNext: Boolean
    def next(): T
    def close(): Unit
  }
  trait Writer[T] {
    def put(obj: T)
    def close(): Unit
  }

  /**
   * Serializes and Deserializes to a bytestream for testing
   */
  def testSerializable[T :ClassTag](before: T)(implicit encoding: Protocol) {
    val baos = new ByteArrayOutputStream()
    val wr = encoding.getWriter[T](new BinOStream(baos))
    wr.put(before)
    wr.close()

    val after = encoding.getReader[T](new BinIStream(new ByteArrayInputStream(baos.toByteArray()))).next()
  }

  /**
   *
   * @see JavaObjectProtocol
   */
  trait Protocol {
    // abstract
    def getReader[T :ClassTag](is: BinIStream): Reader[T]
    def getWriter[T :ClassTag](os: BinOStream): Writer[T]
    
    // concrete
    def getReader[T :ClassTag](path: String): Reader[T] = getReader(inputStream(path))
    def getWriter[T :ClassTag](path: String): Writer[T] = getWriter(outputStream(path))

    // fancy
    def writeOne[T :ClassTag](path: String, obj: T) {
      val wr = getWriter(path)
      wr.put(obj)
      wr.close()
    }
    def readOne[T :ClassTag](path: String): T = {
      val rdr = getReader(path)
      val first = rdr.next()
      rdr.close()
      first
    }
  }
  // TODO ThriftObjectProtocol()
}

