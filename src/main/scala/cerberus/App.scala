package cerberus

import cerberus.exec.DRMAAJobService

object App {
  def main(args: Array[String]) {
    // make sure thrift works
    assert(cerberus.mgmt.Constants.ArbitraryValue == 1)

    println("Hello World!")

    //val qsub = new DRMAAJobService
    //val jobId = qsub.spawnJob("cerberus.HelloWorld", Array("alpha", "beta"))

    //println("spawned "+jobId)
  }
}

object HelloWorld {
  def main(args: Array[String]) {
    println("Hello World!")
    println(args.mkString("`","', `","'"))
  }
}

