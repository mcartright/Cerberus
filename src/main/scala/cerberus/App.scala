package cerberus

case class WordPos(term: String, pos: Int)
case class Posting(term: String, positions: Array[Int])

object App {
  def main(args: Array[String]) {
    val inputFiles = IndexedSeq("data/tiny/doc1.txt", "data/tiny/doc2.txt")
    val files = new SeqFlow(inputFiles)

    // this distributes the data to 2 files starting with prefix split, and returns the names
    val distrib1 = files.splitrr("split", 2)

    // this would happen remotely
    val openedDistrib1 = distrib1.map(df => new FileFlow[String](df))

    // cuz ugly
    def readLines(path: String) = scala.io.Source.fromFile(path).getLines

    // on each node:
    val wordsFlow = new RoundRobinReduceFlow( openedDistrib1.map {
      _.flatMap { filePath => 
        val words = readLines(filePath).mkString(" ").split("\\s+").zipWithIndex.map {
          case(term, idx) => WordPos(term, idx)
        }
        new SeqFlow(words)
      }
    })

    // hacking into scalaville
    println(wordsFlow.toArray.toSet.mkString(","))
  }
}

