package cerberus

object App {

  def main(args: Array[String]) {
    // make sure thrift works
    assert(cerberus.mgmt.Constants.ArbitraryValue == 1)

    println("Hello World!")
  }
}
