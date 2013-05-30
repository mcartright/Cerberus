package cerberus.io

import scala.reflect.ClassTag
import java.io._
import java.net._

object Net {
  def hostName = InetAddress.getLocalHost.getCanonicalHostName
}

object JSocket {
  def apply(address: String, port: Int) = {
    new JSocket(new Socket(InetAddress.getByName(address), port))
  }
  def apply(skt: Socket) = new JSocket(skt)
}

class JSocket(val socket: Socket) {
  // the laziness prevents a deadlock by both endpoints open the same kind of stream at the same time
  lazy val in = new ObjectInputStream(new DataInputStream(socket.getInputStream()))
  lazy val out = new ObjectOutputStream(new DataOutputStream(socket.getOutputStream()))

  // typed read, write, close
  def read[T <:Encodable :ClassTag]() = in.readObject().asInstanceOf[T]
  def write[T <:Encodable :ClassTag](obj:T) = out.writeObject(obj)
  def close() = socket.close()
}

object JServer {
  def apply(port: Int=0) = new JServer(port)
}

class JServer(reqPort: Int) {
  val skt = new ServerSocket(reqPort)
  def hostName = Net.hostName
  def port = skt.getLocalPort
  
  def accept(): JSocket = JSocket(skt.accept())
}
