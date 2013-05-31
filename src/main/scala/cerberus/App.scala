package cerberus

import cerberus.io._
import cerberus.exec._
import cerberus.service.Time

case class FooBar(foo: String, bar: java.lang.Integer)

object App {
  implicit val sharedConf = new SharedConfig
  val cfg = new RuntimeConfig("test", sharedConf)

  implicit val encoding: Protocol = JavaObjectProtocol()
  
  /*
  def runTest(testFn: String=>Unit) {
    val scratchFile = cfg.nextScratchName()
    testFn(scratchFile)
    Util.delete(scratchFile)
  }

  def serializationTest(scratchFile: String) {
    val outputData = Array("Hello!", "There!")
    
    Executor(
      TraversableSource(outputData),
      new FileNode[String](scratchFile)
    ).run(cfg)

    // since a reader is an iterator, we can slurp it into an array
    val writtenData = encoding.getReader[String](scratchFile).toArray
    
    assert(writtenData.sameElements(outputData))
  }

  def caseClassTest(scratchFile: String) {
    val outputData = Array(FooBar("alpha",1), FooBar("beta",2))
    
    Executor(
      TraversableSource(outputData),
      new FileNode[FooBar](scratchFile)
    ).run(cfg)

    println(scratchFile)

    // since a reader is an iterator, we can slurp it into an array
    val writtenData = encoding.getReader[FooBar](scratchFile).toArray
    assert(writtenData.sameElements(outputData))
  }

  def mapFilterTest(scratchFile: String) {
    val inputData = (0 until 10000)
    val outputData = inputData.map(_*3).filter(_<100).toArray
    
    // test ad-hoc init and close handlers
    var wasInit = false
    var wasClosed = false
    
    val stream = {
      val writer = new FileNode[Int](scratchFile)
      val filterer = new FilteredNode[Int](writer, _<100)
      //val mapper = new MappedNode[Int, Int](filterer, x => x*3)
      val mapper = new MappedNode(filterer, new FlowFunction[Int,Int] {
        override def init() { wasInit = true }
        def apply(x: Int): Int = x*3
        override def close() { wasClosed = true }
      })
      mapper
    }
    Executor(TraversableSource(inputData), stream).run(cfg)

    assert(wasInit && wasClosed)

    // since a reader is an iterator, we can slurp it into an array
    val writtenData = encoding.getReader[Int](scratchFile).toArray
    assert(writtenData.sameElements(outputData))
  }

  def bigSortTest(scratchFile: String) {
    val inputData = (0 until 100000).toArray.reverse
    
    val stream = {
      new SortedNode(new FileNode[Int](scratchFile), 10)
    }
    Executor(TraversableSource(inputData), stream).run(cfg)
    
    // since a reader is an iterator, we can slurp it into an array
    val writtenData = encoding.getReader[Int](scratchFile).toArray
    assert(writtenData.sameElements(inputData.sorted))
  }

  def mergeTest() {
    val inputData = (0 until 100000).toArray.reverse
    val outputData = inputData.sorted.toArray

    val numPaths = 10
    val inPathNames = (0 until numPaths).map(_ => cfg.nextScratchName()).toSet
    val outPathNames = inPathNames.map(_ => cfg.nextScratchName()).toSet
    val mergedName = cfg.nextScratchName()

    val jobDispatch = new JobDispatcher

    // split the input data round robin
    jobDispatch.runSync(
      TraversableSource(inputData.toSeq),
      new MappedNode(new RoundRobinDistribNode[Int](inPathNames), (x:Int) => x),
      "split"
    )
    
    assert(encoding.getReader[Int](inPathNames.head).nonEmpty)

    // merge simply, sorting stupidly on the single node endpoint
    jobDispatch.runSync(
      MergedFileSource[Int](inPathNames),
      new SortedNode(new FileNode[Int](mergedName)),
      "mergeSingleNode"
    )

    // make sure that produced the right output
    assert(outputData.deep == encoding.getReader[Int](mergedName).toArray.deep)
    // clean up intermediate file
    Util.delete(mergedName)

    // merge in the parallel nodes and then in the sequential
    val mergeJobs = inPathNames.zip(outPathNames).zipWithIndex.map {
      case ((inPath, outPath),idx) => {
        jobDispatch.run(
          FileSource[Int](inPath),
          new SortedNode(new FileNode[Int](outPath)),
          "merge-"+idx
        )
      }
    }
    jobDispatch.awaitMany(mergeJobs.toSet)

    jobDispatch.runSync(
      SortedMergeSource[Int](outPathNames),
      new FileNode[Int](mergedName),
      "merge-of-sorted"
    )

    // make sure that produced the right output
    assert(outputData.deep == encoding.getReader[Int](mergedName).toArray.deep)
    
    // clean up all files
    Util.delete(mergedName)
    inPathNames.foreach(Util.delete(_))
    outPathNames.foreach(Util.delete(_))
  }

  def awkwardMergeTest() {
    val jobDispatch = new JobDispatcher

    val inputA = (0 until 10).reverse.toArray
    val inputB = (11 until 40).reverse.toArray
    val outputData = (inputA ++ inputB).sorted.toArray

    val fileA = cfg.nextScratchName()
    val fileB = cfg.nextScratchName()
    val fileC = cfg.nextScratchName()

    // split the input data round robin
    jobDispatch.runSync(
      TraversableSource(inputA.toSeq),
      new SortedNode(new FileNode[Int](fileA)),
      "fileA"
    )
    println(fileA)
    jobDispatch.runSync(
      TraversableSource(inputB.toSeq),
      new SortedNode(new FileNode[Int](fileB)),
      "fileB"
    )
    println(fileB)

    // merge simply, sorting stupidly on the single node endpoint
    jobDispatch.runSync(
      SortedMergeSource[Int](Set(fileA, fileB)),
      new FileNode[Int](fileC),
      "mergeAB"
    )
    println(fileC)

    val mergedData = encoding.getReader[Int](fileC).toArray
    println(mergedData.mkString("mergedData(",",",")"))

    // make sure that produced the right output
    assert(outputData.deep == mergedData.deep)
    
    // clean up all files
    Util.delete(fileA)
    Util.delete(fileB)
    Util.delete(fileC)
  }

  def runTests() {
    println("Serialization:")
    runTest(serializationTest)
    println("Case Class:")
    runTest(caseClassTest)
    println("MapFilter:")
    runTest(mapFilterTest)
    println("BigSort:")
    runTest(bigSortTest)

    println("AwkMerge:")
    awkwardMergeTest()
    println("Merge:")
    mergeTest()
  }
  */

  def main(args: Array[String]) {
    //runTests()
    println("Hello World!")
  }
}

