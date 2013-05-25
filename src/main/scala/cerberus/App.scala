package cerberus

import cerberus.io._

object App {
  def serializationTest() {
    val outputData = Array("Hello!", "There!")
    val scratchFile = "/tmp/testStr.flow"
    val fp = new FileNode[String](scratchFile, JavaObjectProtocol())
    outputData.foreach(fp.process(_))
    fp.flush()

    // since a reader is an iterator, we can slurp it into an array
    val writtenData = JavaObjectProtocol().getReader[String](scratchFile).toArray
    assert(writtenData(0) == outputData(0))
    assert(writtenData(1) == outputData(1))
  }
  def mapFilterTest() {
    val inputData = (0 until 10000)
    val scratchFile = "/tmp/testMapFilter.flow"
    val stream = {
      val writer = new FileNode[java.lang.Integer](scratchFile, JavaObjectProtocol())
      val filterer = new FilteredNode[java.lang.Integer](writer, x => x < 100)
      val mapper = new MappedNode[java.lang.Integer, java.lang.Integer](filterer, x => x*3)
      mapper
    }
    inputData.foreach(stream.process(_))
    stream.flush()

    // since a reader is an iterator, we can slurp it into an array
    val writtenData = JavaObjectProtocol().getReader[java.lang.Integer](scratchFile).toArray
    assert(writtenData.last == 99)
    assert(writtenData(0) == 0)
    assert(writtenData(1) == 3)
    //println(writtenData.mkString(", "))
  }
  def main(args: Array[String]) {
    serializationTest()
    mapFilterTest()
    println("Hello World!")
  }
}

