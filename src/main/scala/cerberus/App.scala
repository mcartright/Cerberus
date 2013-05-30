package cerberus

import cerberus.io._
import cerberus.exec.Time

case class FooBar(foo: String, bar: java.lang.Integer) extends Encodable {
}

object App {
  type JInt = java.lang.Integer
  val cfg = new RuntimeConfig("test")
  implicit val encoding: Protocol = JavaObjectProtocol()
  
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
      val writer = new FileNode[JInt](scratchFile)
      val filterer = new FilteredNode[JInt](writer, x => x < 100)
      //val mapper = new MappedNode[JInt, JInt](filterer, x => x*3)
      val mapper = new MappedNode[JInt, JInt](filterer, new FlowFunction[JInt,JInt] {
        override def init() { wasInit = true }
        def apply(x: JInt): JInt = x*3
        override def close() { wasClosed = true }
      })
      mapper
    }
    Executor(TraversableSource(inputData.map(new JInt(_))), stream).run(cfg)

    assert(wasInit && wasClosed)

    // since a reader is an iterator, we can slurp it into an array
    val writtenData = encoding.getReader[JInt](scratchFile).toArray
    assert(writtenData.sameElements(outputData))
  }

  def bigSortTest(scratchFile: String) {
    val inputData = (0 until 100000).map(new JInt(_)).toArray.reverse
    
    val stream = {
      new SortedNode[JInt](new FileNode[JInt](scratchFile), 10)
    }
    Executor(TraversableSource(inputData), stream).run(cfg)
    
    // since a reader is an iterator, we can slurp it into an array
    val writtenData = encoding.getReader[JInt](scratchFile).toArray
    assert(writtenData.sameElements(inputData.sorted))
  }

  def mergeTest() {
    val inputData = (0 until 100000).map(new JInt(_)).toArray.reverse
    val outputData = inputData.sorted.toArray

    val numPaths = 10
    val inPathNames = (0 until numPaths).map(_ => cfg.nextScratchName()).toSet
    val outPathNames = inPathNames.map(_ => cfg.nextScratchName()).toSet
    val mergedName = cfg.nextScratchName()

    val jobDispatch = new JobDispatcher

    // split the input data round robin
    jobDispatch.runSync(
      TraversableSource(inputData.toSeq),
      new MappedNode(new RoundRobinDistribNode[JInt](inPathNames), (x:JInt) => x),
      "split"
    )
    
    assert(encoding.getReader[JInt](inPathNames.head).nonEmpty)

    // merge simply, sorting stupidly on the single node endpoint
    jobDispatch.runSync(
      MergedFileSource[JInt](inPathNames),
      new SortedNode[JInt](new FileNode[JInt](mergedName)),
      "mergeSingleNode"
    )

    // make sure that produced the right output
    assert(outputData.deep == encoding.getReader[JInt](mergedName).toArray.deep)
    // clean up intermediate file
    Util.delete(mergedName)

    // merge in the parallel nodes and then in the sequential
    val mergeJobs = inPathNames.zip(outPathNames).zipWithIndex.map {
      case ((inPath, outPath),idx) => {
        jobDispatch.run(
          FileSource[JInt](inPath),
          new SortedNode[JInt](new FileNode[JInt](outPath)),
          "merge-"+idx
        )
      }
    }
    jobDispatch.awaitMany(mergeJobs.toSet)

    jobDispatch.runSync(
      SortedMergeSource[JInt](outPathNames),
      new FileNode[JInt](mergedName),
      "merge-of-sorted"
    )

    // make sure that produced the right output
    assert(outputData.deep == encoding.getReader[JInt](mergedName).toArray.deep)
    
    // clean up all files
    Util.delete(mergedName)
    inPathNames.foreach(Util.delete(_))
    outPathNames.foreach(Util.delete(_))
  }

  def awkwardMergeTest() {
    val jobDispatch = new JobDispatcher

    val inputA = (0 until 10).map(new JInt(_)).reverse.toArray
    val inputB = (11 until 40).map(new JInt(_)).reverse.toArray
    val outputData = (inputA ++ inputB).sorted.toArray

    val fileA = cfg.nextScratchName()
    val fileB = cfg.nextScratchName()
    val fileC = cfg.nextScratchName()

    // split the input data round robin
    jobDispatch.runSync(
      TraversableSource(inputA.toSeq),
      new SortedNode(new FileNode[JInt](fileA)),
      "fileA"
    )
    jobDispatch.runSync(
      TraversableSource(inputB.toSeq),
      new SortedNode(new FileNode[JInt](fileB)),
      "fileB"
    )

    // merge simply, sorting stupidly on the single node endpoint
    jobDispatch.runSync(
      SortedMergeSource[JInt](Set(fileA, fileB)),
      new FileNode[JInt](fileC),
      "mergeAB"
    )

    val mergedData = encoding.getReader[JInt](fileC).toArray
    println(mergedData.mkString("mergedData(",",",")"))

    // make sure that produced the right output
    assert(outputData.deep == mergedData.deep)
    
    // clean up all files
    Util.delete(fileA)
    Util.delete(fileB)
    Util.delete(fileC)
  }

  def runTests() {
    runTest(serializationTest)
    runTest(caseClassTest)
    runTest(mapFilterTest)
    runTest(bigSortTest)

    awkwardMergeTest()
    mergeTest()
  }

  def main(args: Array[String]) {
    runTests()
  }
}

