package cerberus

import cerberus.io._

object App {
  type JInt = java.lang.Integer
  val cfg = new RuntimeConfig("test")
  val testEncoding = JavaObjectProtocol()
  
  def runTest(testFn: String=>Unit) {
    val scratchFile = cfg.nextScratchName()
    testFn(scratchFile)
    Util.delete(scratchFile)
  }

  def serializationTest(scratchFile: String) {
    val outputData = Array("Hello!", "There!")
    
    Executor(
      new TraversableSource(outputData),
      new FileNode[String](scratchFile, testEncoding)
    ).run(cfg)

    // since a reader is an iterator, we can slurp it into an array
    val writtenData = testEncoding.getReader[String](scratchFile).toArray
    
    assert(writtenData.sameElements(outputData))
  }
  
  def mapFilterTest(scratchFile: String) {
    val inputData = (0 until 10000)
    val outputData = inputData.map(_*3).filter(_<100).toArray
    
    val stream = {
      val writer = new FileNode[JInt](scratchFile, testEncoding)
      val filterer = new FilteredNode[JInt](writer, x => x < 100)
      val mapper = new MappedNode[JInt, JInt](filterer, x => x*3)
      mapper
    }
    Executor(new TraversableSource(inputData.map(new JInt(_))), stream).run(cfg)
    
    // since a reader is an iterator, we can slurp it into an array
    val writtenData = testEncoding.getReader[JInt](scratchFile).toArray
    assert(writtenData.sameElements(outputData))
  }

  def bigSortTest(scratchFile: String) {
    val inputData = (0 until 100000).map(new JInt(_)).toArray.reverse
    
    val stream = {
      val writer = new FileNode[JInt](scratchFile, testEncoding)
      val sorter = new SortedNode[JInt](writer, testEncoding, 10)
      sorter
    }
    Executor(new TraversableSource(inputData), stream).run(cfg)
    
    // since a reader is an iterator, we can slurp it into an array
    val writtenData = testEncoding.getReader[JInt](scratchFile).toArray
    assert(writtenData.sameElements(inputData.sorted))
  }

  def main(args: Array[String]) {
    runTest(serializationTest)
    runTest(mapFilterTest)
    runTest(bigSortTest)
    
    println("Hello World!")
  }
}

