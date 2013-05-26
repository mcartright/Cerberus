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
      TraversableSource(outputData),
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
    Executor(TraversableSource(inputData.map(new JInt(_))), stream).run(cfg)
    
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
    Executor(TraversableSource(inputData), stream).run(cfg)
    
    // since a reader is an iterator, we can slurp it into an array
    val writtenData = testEncoding.getReader[JInt](scratchFile).toArray
    assert(writtenData.sameElements(inputData.sorted))
  }

  def mergeTest() {
    val inputData = (0 until 100).map(new JInt(_)).toArray.reverse
    val outputData = inputData.sorted.toArray

    val numPaths = 10
    val inPathNames = (0 until numPaths).map(_ => cfg.nextScratchName()).toSet
    val outPathNames = inPathNames.map(_ => cfg.nextScratchName()).toSet
    val mergedName = cfg.nextScratchName()

    // split the input data round robin
    Executor(
      TraversableSource(inputData.toSeq),
      new RoundRobinDistribNode[JInt](inPathNames, testEncoding)
    ).run(cfg)

    // merge simply, sorting stupidly on the single node endpoint
    Executor(
      MergedFileSource[JInt](inPathNames, testEncoding),
      new SortedNode[JInt](new FileNode[JInt](mergedName, testEncoding), testEncoding)
    ).run(cfg)

    // make sure that produced the right output
    assert(outputData.deep == testEncoding.getReader[JInt](mergedName).toArray.deep)
    // clean up intermediate file
    Util.delete(mergedName)

    // merge in the parallel nodes and then in the sequential
    inPathNames.zip(outPathNames).foreach {
      case(inPath, outPath) =>
        Executor(
          FileSource[JInt](inPath, testEncoding),
          new SortedNode[JInt](new FileNode[JInt](outPath, testEncoding), testEncoding)
        ).run(cfg)
    }
    Executor(
      SortedMergeSource[JInt](outPathNames, testEncoding),
      new FileNode[JInt](mergedName, testEncoding)
    ).run(cfg)

    // make sure that produced the right output
    assert(outputData.deep == testEncoding.getReader[JInt](mergedName).toArray.deep)
    
    // clean up all files
    Util.delete(mergedName)
    inPathNames.foreach(Util.delete(_))
    outPathNames.foreach(Util.delete(_))
  }

  def main(args: Array[String]) {
    runTest(serializationTest)
    runTest(mapFilterTest)
    runTest(bigSortTest)

    mergeTest()
    
    println("Hello World!")
  }
}

