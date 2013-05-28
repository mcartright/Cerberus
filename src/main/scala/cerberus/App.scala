package cerberus

import cerberus.io._
import cerberus.exec.Time

object App {
  type JInt = java.lang.Integer
  val cfg = new RuntimeConfig("test")
  
  def runTest(testFn: String=>Unit) {
    val scratchFile = cfg.nextScratchName()
    testFn(scratchFile)
    Util.delete(scratchFile)
  }

  def serializationTest(scratchFile: String) {
    implicit val encoding: Protocol = JavaObjectProtocol()
    val outputData = Array("Hello!", "There!")
    
    Executor(
      TraversableSource(outputData),
      new FileNode[String](scratchFile)
    ).run(cfg)

    // since a reader is an iterator, we can slurp it into an array
    val writtenData = encoding.getReader[String](scratchFile).toArray
    
    assert(writtenData.sameElements(outputData))
  }
  
  def mapFilterTest(scratchFile: String) {
    implicit val encoding: Protocol = JavaObjectProtocol()
    val inputData = (0 until 10000)
    val outputData = inputData.map(_*3).filter(_<100).toArray
    
    val stream = {
      val writer = new FileNode[JInt](scratchFile)
      val filterer = new FilteredNode[JInt](writer, x => x < 100)
      val mapper = new MappedNode[JInt, JInt](filterer, x => x*3)
      mapper
    }
    Executor(TraversableSource(inputData.map(new JInt(_))), stream).run(cfg)
    
    // since a reader is an iterator, we can slurp it into an array
    val writtenData = encoding.getReader[JInt](scratchFile).toArray
    assert(writtenData.sameElements(outputData))
  }

  def bigSortTest(scratchFile: String) {
    implicit val encoding: Protocol = JavaObjectProtocol()
    val inputData = (0 until 100000).map(new JInt(_)).toArray.reverse
    
    println(">>>>")
    val stream = {
      new SortedNode[JInt](new FileNode[JInt](scratchFile), 10)
    }
    Executor(TraversableSource(inputData), stream).run(cfg)
    
    // since a reader is an iterator, we can slurp it into an array
    val writtenData = encoding.getReader[JInt](scratchFile).toArray
    assert(writtenData.sameElements(inputData.sorted))
  }

  def mergeTest() {
    implicit val encoding: Protocol = JavaObjectProtocol()
    val inputData = (0 until 100).map(new JInt(_)).toArray.reverse
    val outputData = inputData.sorted.toArray

    val numPaths = 10
    val inPathNames = (0 until numPaths).map(_ => cfg.nextScratchName()).toSet
    val outPathNames = inPathNames.map(_ => cfg.nextScratchName()).toSet
    val mergedName = cfg.nextScratchName()

    val jobDispatch = new JobDispatcher

    // split the input data round robin
    jobDispatch.runSync(
      TraversableSource(inputData.toSeq),
      new RoundRobinDistribNode[JInt](inPathNames),
      "split"
    )
   /*
   Executor(
     TraversableSource(inputData),
     new RoundRobinDistribNode[JInt](inPathNames)
   ).run(cfg)
   */
    
    assert(encoding.getReader[JInt](inPathNames.head).nonEmpty)

    // merge simply, sorting stupidly on the single node endpoint
    jobDispatch.runSync(
      MergedFileSource[JInt](inPathNames),
      new SortedNode[JInt](new FileNode[JInt](mergedName)),
      "mergeStupid"
    )

    // make sure that produced the right output
    assert(outputData.deep == encoding.getReader[JInt](mergedName).toArray.deep)
    // clean up intermediate file
    Util.delete(mergedName)

    // merge in the parallel nodes and then in the sequential
    inPathNames.zip(outPathNames).foreach {
      case(inPath, outPath) =>
        Executor(
          FileSource[JInt](inPath),
          new SortedNode[JInt](new FileNode[JInt](outPath))
        ).run(cfg)
    }
    Executor(
      SortedMergeSource[JInt](outPathNames),
      new FileNode[JInt](mergedName)
    ).run(cfg)

    // make sure that produced the right output
    assert(outputData.deep == encoding.getReader[JInt](mergedName).toArray.deep)
    
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

