package cerberus

import cerberus.io._

object App {
  def main(args: Array[String]) {
    val outputData = Array("Hello!", "There!")
    val scratchFile = "/tmp/testStr.flow"
    val fp = new FileNode[String](scratchFile, JavaObjectProtocol())
    outputData.foreach(fp.process(_))
    fp.flush()

    // since a reader is an iterator, we can slurp it into an array
    val writtenData = JavaObjectProtocol().getReader[String](scratchFile).toArray
    assert(writtenData(0) == outputData(0))
    assert(writtenData(1) == outputData(1))
    
    println("Hello World!")
  }
}

