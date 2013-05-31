package cerberus

import cerberus.io._
import cerberus.exec._
import cerberus.service._

case class FooBar(foo: String, bar: java.lang.Integer)

object App {
  implicit val sharedConf = new SharedConfig
  implicit val enc: Protocol = JavaObjectProtocol()
  val qsub = new LocalJobService

  def caseClassTest() {
    val scratchFile = "/tmp/foobar"
    val foobar = FooBar("alpha", 1)
    
    enc.writeOne(scratchFile, foobar)
    assert(foobar == enc.readOne[FooBar](scratchFile))

    Util.delete(scratchFile)
  }

  def splitFileTest() {
    val jobDispatch = new JobDispatcher(qsub)
    val input = (0 until 100).toSeq
    val outA = sharedConf.makeJobDir("outA")
    
    jobDispatch.run(
      TraversableSource(input),
      new RoundRobinOutputNode[Int](outA),
      outA
    )
    
    val merged = SplitFormat.toArray[Int](outA).sorted
    assert(input.sameElements(merged))

    // clean up working directory
    Util.delete(outA)
  }

  def mapFilterTest() {
    val jobDispatch = new JobDispatcher(qsub)
    val input = (0 until 10000).toSeq
    val output = input.map(_*3).filter(_<100)
    
    val outA = sharedConf.makeJobDir("outA")
    val outB = sharedConf.makeJobDir("outB")
    
    // this task splits the input
    jobDispatch.run(
      TraversableSource(input),
      new RoundRobinOutputNode[Int](outA),
      outA
    )

    // this task processes in parallel
    jobDispatch.run(
      SplitInputSource[Int](outA),
      new MappedNode[Int,Int](new FilteredNode[Int](new SplitOutputNode[Int](outB), _<100), _*3),
      outB
    )
    
    // re-impose an ordering by sorting...
    val merged = SplitFormat.toArray[Int](outB).sorted
    assert(output.sameElements(merged))

    // clean up working dirs
    Util.delete(outA)
    Util.delete(outB)
  }

  /*
  def mapFilterSugarTest() {
    val jobDispatch = new JobDispatcher(qsub)
    val input = (0 until 10000).toSeq
    val output = input.map(_*3).filter(_<100)
    
    val outA = sharedConf.makeJobDir("outA")
    val outB = sharedConf.makeJobDir("outB")

    val base = TraversableFlow(input)
    base.split.run(outA)


    base.split.map(_*3).filter(_<100)
  }
  */

  def bigSortTest() {
    val jobDispatch = new JobDispatcher(qsub)
    val input = (0 until 100000).reverse
    
    // put sortedFile in jobDir so it gets cleaned up together
    val jobDir = sharedConf.makeJobDir("bigsort")
    val sortedFile = jobDir+"/sortedFile"
    
    // restrict Sort to buffers of size 10 to test it's merging
    jobDispatch.run(
      TraversableSource(input),
      new SortedNode(new RoundRobinOutputNode[Int](jobDir), 10),
      "split-sort"
    )

    // generally you run a reduce or foreach on a sorted stream
    // this will read it sequentially
    jobDispatch.run(
      SortedMergeSource[Int](jobDir),
      new ScratchFileNode[Int](sortedFile),
      "merge-sort"
    )

    assert((0 until 100000).sameElements(enc.getReader[Int](sortedFile).toSeq))
    
    Util.delete(jobDir)
  }

  def runTests() {
    caseClassTest
    splitFileTest()
    mapFilterTest()
    bigSortTest()
  }

  def main(args: Array[String]) {
    runTests()
  }
}

