package cerberus.io

import scala.reflect.ClassTag

object SplitFormat {
  def dataPath(path: String, id: Int) = "%s/%d.data".format(path,id)
  def metadataPath(path: String, id: Int) = "%s/%d.meta".format(path, id)

  def toArray[T: ClassTag](path: String)(implicit enc: Protocol): Array[T] = {
    val rdrs = allReaders(path)
    val size = rdrs.map(_.count).sum
    
    // fill up array with all the elements
    val data = new Array[T](size.toInt)
    var idx = 0
    rdrs.foreach(_.foreach(el => {
      data(idx) = el
      idx += 1
    }))

    // close readers
    rdrs.foreach(_.close())

    // return array
    data
  }
  
  /**
   * Needs to be closed
   */
  def toReader[T: ClassTag](path: String)(implicit enc: Protocol): Reader[T] = new Reader[T] {
    val rdrs = allReaders(path)
    val iter = rdrs.map(_.asInstanceOf[Iterator[T]]).reduceLeft(_ ++ _)
    def hasNext = iter.hasNext
    def next() = iter.next()
    def close() = rdrs.foreach(_.close())
  }

  def allReaders[T :ClassTag](path: String)(implicit enc: Protocol): IndexedSeq[SplitReader[T]] = {
    // pop open 0th metadata to find out how many nodes there are
    val metadata = enc.readOne[FileSplitMetadata](metadataPath(path, 0))
    val numNodes = metadata.numNodes
    assert(metadata.className == implicitly[ClassTag[T]].runtimeClass.getName)
    
    (0 until numNodes).map { id =>
      new SplitReader[T](path, id, numNodes)
    }.toIndexedSeq
  }

  def allWriters[T :ClassTag](path: String, numNodes: Int)(implicit enc: Protocol): IndexedSeq[Writer[T]] = {
    (0 until numNodes).map { id =>
      new SplitWriter[T](path, id, numNodes)
    }.toIndexedSeq
  }
}

object FileSplitMetadata {
  def apply[T :ClassTag](count: Long, id: Int, nodeCount: Int) = 
    new FileSplitMetadata(count, id, nodeCount, implicitly[ClassTag[T]].runtimeClass.getName)
}

case class FileSplitMetadata(val dataCount: Long, val nodeId: Int, val numNodes: Int, val className: String)

/**
 * Need not be encodable
 */
class SplitWriter[T :ClassTag](path: String, nodeId: Int, numNodes: Int)(implicit encoding: Protocol) extends Writer[T] {
  var count = 0L
  Util.mkdir(path)
  val fp = encoding.getWriter(SplitFormat.dataPath(path, nodeId))
  
  def put(next: T) {
    fp.put(next)
    count += 1
  }
  def close() {
    fp.close()
    // spit out a metadata file
    encoding.writeOne(
      SplitFormat.metadataPath(path, nodeId),
      FileSplitMetadata(count, nodeId, numNodes)
    )
  }
}

class SplitReader[T :ClassTag](path: String, nodeId: Int, numNodes: Int)(implicit encoding: Protocol) extends Reader[T] {
  val metadata = encoding.readOne[FileSplitMetadata](SplitFormat.metadataPath(path, nodeId))
  val fp = encoding.getReader[T](SplitFormat.dataPath(path, nodeId))
  
  def count: Long = metadata.dataCount
  def next() = fp.next()
  def hasNext = fp.hasNext
  def close() = fp.close()
}

