package cerberus

import cerberus.io._

object App {
  type JInt = java.lang.Integer
  val cfg = new RuntimeConfig("test")
  val testEncoding = JavaObjectProtocol()
  
  def serializationTest() {
    val outputData = Array("Hello!", "There!")
    val scratchFile = cfg.nextScratchName()
    val fp = new FileNode[String](scratchFile, testEncoding)
    outputData.foreach(fp.process(_))
    fp.flush()

    // since a reader is an iterator, we can slurp it into an array
    val writtenData = testEncoding.getReader[String](scratchFile).toArray
    assert(writtenData(0) == outputData(0))
    assert(writtenData(1) == outputData(1))
  }
  
  def mapFilterTest() {
    val inputData = (0 until 10000)
    val scratchFile = cfg.nextScratchName()
    val stream = {
      val writer = new FileNode[JInt](scratchFile, testEncoding)
      val filterer = new FilteredNode[JInt](writer, x => x < 100)
      val mapper = new MappedNode[JInt, JInt](filterer, x => x*3)
      mapper
    }
    inputData.foreach(stream.process(_))
    stream.flush()
    
    // since a reader is an iterator, we can slurp it into an array
    val writtenData = testEncoding.getReader[JInt](scratchFile).toArray
    assert(writtenData.last == 99)
    assert(writtenData(0) == 0)
    assert(writtenData(1) == 3)
    //println(writtenData.mkString(", "))
  }

  def unbufferedSortTest() {
    val inputData = (0 until 10).reverse.toArray
    val scratchFile = cfg.nextScratchName()
    
    val stream = {
      val writer = new FileNode[JInt](scratchFile, testEncoding)
      val sorter = new SortedNode[JInt](writer, testEncoding, 11)
      sorter
    }
    stream.conf(cfg)
    inputData.foreach(stream.process(_))
    stream.flush()
    
    // since a reader is an iterator, we can slurp it into an array
    val writtenData = testEncoding.getReader[JInt](scratchFile).toArray
    assert(writtenData.sameElements(inputData.sorted))
  }

  def bigSortTest() {
    val inputData = (0 until 100000).toArray.reverse
    val scratchFile = cfg.nextScratchName()
    
    val stream = {
      val writer = new FileNode[JInt](scratchFile, testEncoding)
      val sorter = new SortedNode[JInt](writer, testEncoding, 10)
      sorter
    }
    stream.conf(cfg)
    inputData.foreach(stream.process(_))
    stream.flush()
    
    // since a reader is an iterator, we can slurp it into an array
    val writtenData = testEncoding.getReader[JInt](scratchFile).toArray
    assert(writtenData.sameElements(inputData.sorted))
  }

  def main(args: Array[String]) {
    serializationTest()
    mapFilterTest()
    unbufferedSortTest()
    bigSortTest()
    println("Hello World!")
  }
}

