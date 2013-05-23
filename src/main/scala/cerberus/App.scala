package cerberus

object App {
  def main(args: Array[String]) {
    val inputFiles = IndexedSeq("data/tiny/doc1.txt", "data/tiny/doc2.txt")
    val files = new SeqFlow(inputFiles)

    files.map(path => path.toUpperCase).write("out1.flow")
    
    val postSerialized = new FileFlow[String]("out1.flow").map(_.toLowerCase).toArray

    assert(postSerialized(0) == inputFiles(0))
    assert(postSerialized(1) == inputFiles(1))

  }
}

