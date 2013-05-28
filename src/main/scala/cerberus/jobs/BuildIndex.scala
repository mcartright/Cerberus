package cerberus.jobs

import cerberus.io._
import scala.io.Source
import scala.math.Ordered
import java.io._

case class CountedSplit(val filename: String, val count: Int)
case class OffsetSplit(val file: String, val count: Int, val start: Int)
case class Doc(
  val id: Int,
  val name: String,
  val content: String,
  val text: Seq[String]
)
case class IdLength(val id: Int, val length: Int)
case class IdName(val id: Int, val name: String)
case class Posting(
  val term: String,
  val doc: Int,
  val positions: Array[Int]
) extends Ordered[Posting] {
  def compare(that: Posting): Int = {
    var r = this.term compare that.term
    if (r != 0) return r
    this.doc - that.doc
  }
}

// Define the writer function
abstract class Writer[T1](
  val dst: String,
  val suffix: String
) extends Function1[T1, Unit] with Serializable {
  @transient lazy val dstFile = new FileWriter(dst + suffix)
  def close() = dstFile.close()
  def apply(obj: T1): Unit
}

object BuildIndex {
  // stateful offsetting function
  val offset = new Function1[CountedSplit, OffsetSplit] {
    var total = 0
    def apply(cs: CountedSplit): OffsetSplit = {
      val toReturn = OffsetSplit(cs.filename, cs.count, total)
      total += cs.count
      toReturn
    }
  }

  def count(fn: String) = {
    val headers = Source.fromFile(fn).getLines.filter { _ == "<DOC>" }
    CountedSplit(fn, headers.size)
  }

  def intoDocuments(of: OffsetSplit): Seq[Doc] = {
    val rawContent =
      Source.fromFile(of.file).getLines.mkString("\n").split("</DOC>")
    val numbered = rawContent.zipWithIndex.map { case (d, i) =>
        val id = i + of.start
        val name =
          """<DOCNO>(.+)</DOCNO""".r.findFirstMatchIn(d).get.group(1).trim
        Doc(id, name, d, Seq.empty)
    }
    numbered.toSeq
  }

  def tokenize(d: Doc): Doc = {
    d.copy(text = """\s""".r.split(d.content))
  }

  def normalize(d: Doc): Doc = {
    d.copy(text = d.text.map(_.toLowerCase))
  }
  
  def getPostings(d: Doc) = {
    val termPositions = d.text.zipWithIndex.groupBy(_._1)
    val postings = termPositions.keys.map { term =>
      val pos = termPositions(term).map(_._2).sorted.toArray
      Posting(term, d.id, pos)
    }
    postings
  }
    
  def namesWriter(prefix: String) = new Writer[IdName](prefix, "names") {
    def apply(p: IdName): Unit = dstFile.write(s"${p.id}\t${p.name}\n")
  }

  def lengthsWriter(prefix: String) = new Writer[IdLength](prefix, "lengths") {
    def apply(p: IdLength): Unit = dstFile.write(s"${p.id}\t${p.length}\n")
  }

  def postingsWriter(prefix: String) = new Writer[Posting](prefix, "postings") {
    def apply(p: Posting): Unit = {
      val str = s"${p.term}\t${p.doc}\t${p.positions.mkString(",")}\n"
      dstFile.write(str)
    }
  }

  def runLocally(files: Seq[String], dest: String) = {
    Util.mkdir(dest)
    
    // We start with a list of files
    // Fan them out for counting
    val counted = files.par.map(count)

    // OffsetSplitter
    val shifted = counted.seq.map(offset)

    // Three transformations in a row!
    // ParserSelector/UniversalParser & TagTokenizer & A lemmatizer

    val parsedDocuments =
      shifted.par.flatMap(intoDocuments).map(tokenize).map(normalize)

    val writeLengths = lengthsWriter(dest)
    parsedDocuments.map {
      doc => IdLength(doc.id, doc.text.length)
    }.seq.sortBy(_.id).foreach(writeLengths)
    writeLengths.close()

    val writeNames = namesWriter(dest)
    parsedDocuments.map {
      doc => IdName(doc.id, doc.name)
    }.seq.sortBy(_.id).foreach(writeNames)
    writeNames.close()

    val writePostings = postingsWriter(dest)
    parsedDocuments.flatMap {
      doc => getPostings(doc)
    }.seq.sorted.foreach(writePostings)
    writePostings.close()

    // corpus -- fairly redundant - don't worry about this yet
    //parsedDocuments.sorted.foreach(doc => write(doc))

  }

  def main(args: Array[String]) {
    runLocally(Seq("data/test1","data/test2"), "locally-built-index/")
  }
}

