import scala.io.Source
import scala.math.Ordered
import java.io._

// Define the writer function
abstract class Writer[T1, R](val dst: String, val suffix: String)
    extends Function1[T1, R] {
  val dstFile = new FileWriter(dst + suffix)
  def close = dstFile.close
  def apply(obj: T1): R
}

def count(fn: String): Int = {
  val headers =
    Source.fromFile(fn).getLines.filter { l =>
      l == "<DOC>"
    }
  headers.size
}

// We start with a list of files
// DocumentSource
val prefix = "/usr/ayr/tmp1/irmarc/projects/thesis/code/cerberus/data/"
val files =
  Seq(prefix + "test1", prefix + "test2")

// Fan them out for counting
// ParserCounter
case class CountedSplit(val filename: String, val count: Int)
case class OffsetSplit(val file: String, val count: Int, val start: Int)
val counted = files.par.map { f =>
  val num = count(f)
  CountedSplit(f, num)
}

// offsetting function
val offset = new Function1[CountedSplit, OffsetSplit] {
  var total = 0
  def apply(cs: CountedSplit): OffsetSplit = {
    val toReturn = OffsetSplit(cs.filename, cs.count, total)
    total += cs.count
    toReturn
  }
}

// OffsetSplitter
val shifted = counted.seq.map(offset)

// Three transformations in a row!
// ParserSelector/UniversalParser & TagTokenizer & A lemmatizer
case class Doc(
  val id: Int,
  val name: String,
  val content: String,
  val text: Seq[String]
)

val intoDocuments = (of: OffsetSplit) => {
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
val tokenize = (d: Doc) => d.copy(text = """\s""".r.split(d.content))
val normalize = (d: Doc) => d.copy(text = d.text.map(_.toLowerCase))
val parsedDocuments =
  shifted.par.flatMap(intoDocuments).map(tokenize).map(normalize)

case class IdLength(val id: Int, val length: Int)
val writeLengths = new Writer[IdLength, Unit](prefix, "lengths") {
  def apply(p: IdLength): Unit = dstFile.write(s"${p.id}\t${p.length}\n")
}

parsedDocuments.map {
  doc => IdLength(doc.id, doc.text.length)
}.seq.sortBy(_.id).foreach(writeLengths)

writeLengths.close

// names
case class IdName(val id: Int, val name: String)
val writeNames = new Writer[IdName, Unit](prefix, "names") {
  def apply(p: IdName): Unit = dstFile.write(s"${p.id}\t${p.name}\n")
}

parsedDocuments.map {
  doc => IdName(doc.id, doc.name)
}.seq.sortBy(_.id).foreach(writeNames)

writeNames.close

// postings
@serializable case class Posting(
  val term: String,
  val doc: Int,
  val positions: Array[Int]
)
    extends Ordered[Posting] {
  def compare(that: Posting): Int = {
    var r = this.term compare that.term
    if (r != 0) return r
    this.doc - that.doc
  }
}

val getPostings = (d: Doc) => {
  val termPositions = d.text.zipWithIndex.groupBy(_._1)
  val postings = termPositions.keys.map { term =>
    val pos = termPositions(term).map(_._2).sorted.toArray
    Posting(term, d.id, pos)
  }
  postings
}

val writePostings = new Writer[Posting, Unit](prefix, "postings") {
  def apply(p: Posting): Unit = {
    val str = s"${p.term}\t${p.doc}\t${p.positions.mkString(",")}\n"
    dstFile.write(str)
  }
}

parsedDocuments.flatMap {
  doc => getPostings(doc)
}.seq.sorted.foreach(writePostings)

// Boo - how to avoid this?
writePostings.close

// corpus -- fairly redundant - don't worry about this yet
//parsedDocuments.sorted.foreach(doc => write(doc))
