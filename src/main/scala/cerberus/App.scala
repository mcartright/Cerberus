package cerberus

object App {
  def main(args: Array[String]) {
    val inputFiles = IndexedSeq("data/tiny/doc1.txt", "data/tiny/doc2.txt")
    val files = new SeqFlow(inputFiles)

    files.map(path => path.toUpperCase).write("/tmp/out.flow")
    
    val postSerialized = new FileFlow[String]("/tmp/out.flow").map(_.toLowerCase).toArray

    assert(postSerialized(0) == inputFiles(0))
    assert(postSerialized(1) == inputFiles(1))

    val distrib1 = new FileFlow[String]("/tmp/out.flow").map(_.toLowerCase).splitrr("split", 2)

    // this would happen remotely
    val openedDistrib1 = distrib1.map(df => new FileFlow[String](df))

    def readLines(path: String) = scala.io.Source.fromFile(path).getLines

    // on each node:
    val needsReduce = openedDistrib1.map {
      _.flatMap { filePath => 
        val words = readLines(filePath).mkString(" ").split("\\s+")
        //words.foreach(println)
        new SeqFlow(words)
      }
    }

    // hacking into scalaville
    needsReduce.flatMap(_.toArray).toSet.mkString(",")
  }
}

