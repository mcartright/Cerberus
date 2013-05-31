package cerberus

import cerberus.io._
import cerberus.exec._
import cerberus.service.Time

case class FooBar(foo: String, bar: java.lang.Integer)

object App {
  implicit val sharedConf = new SharedConfig
  implicit val enc: Protocol = JavaObjectProtocol()

  def caseClassTest() {
    val scratchFile = "/tmp/foobar"
    val foobar = FooBar("alpha", 1)
    
    enc.writeOne(scratchFile, foobar)
    assert(foobar == enc.readOne[FooBar](scratchFile))

    Util.delete(scratchFile)
  }

  def splitFileTest() {
    val jobDispatch = new JobDispatcher
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
    val jobDispatch = new JobDispatcher
    val input = (0 until 10000).toSeq
    val output = input.map(_*3).filter(_<100)
    
    val outA = sharedConf.makeJobDir("outA")
    val outB = sharedConf.makeJobDir("outB")
    
    jobDispatch.run(
      TraversableSource(input),
      new RoundRobinOutputNode[Int](outA),
      outA
    )

    jobDispatch.runDistributed(
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

  def runTests() {
    caseClassTest
    splitFileTest()
    mapFilterTest()
  }

  def main(args: Array[String]) {
    runTests()
  }
}

