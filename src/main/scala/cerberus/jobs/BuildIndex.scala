package cerberus.jobs

import cerberus._
import cerberus.io._
import scala.io.Source
import scala.math.Ordered
import java.io._

case class CountedSplit(val filename: String, val count: Int) extends Encodable
case class OffsetSplit(val file: String, val count: Int, val start: Int) extends Encodable
case class Doc(
  val id: Int,
  val name: String,
  val content: String,
  val text: Seq[String]
) extends Encodable
case class IdLength(val id: Int, val length: Int) extends Ordered[IdLength] with Encodable {
  def compare(that: IdLength) = id compare that.id
}
case class IdName(val id: Int, val name: String) extends Ordered[IdName] with Encodable {
  def compare(that: IdName) = id compare that.id
}
case class Posting(
  val term: String,
  val doc: Int,
  val positions: Array[Int]
) extends Ordered[Posting] with Encodable {
  override def toString = "Posting("+term+","+doc+",Array("+positions.mkString(",")+"))"
  def compare(that: Posting): Int = {
    var r = this.term compare that.term
    if (r != 0) return r
    if(this.doc == that.doc) {
      println("ERR"+(this.doc, that.doc))
    }
    this.doc - that.doc
  }
}

// Define the writer function
abstract class IndexWriter[T1](
  val dst: String,
  val suffix: String
) extends FlowFunction[T1, Unit] {
  @transient lazy val dstFile = new FileWriter(dst + suffix)
  override def close() {
    dstFile.close()
  }
  def apply(obj: T1): Unit
}

class Offsetter extends FlowFunction[CountedSplit, OffsetSplit] {
  @transient var total = 0
  def apply(cs: CountedSplit): OffsetSplit = {
    println(total)
    println(cs)
    val toReturn = OffsetSplit(cs.filename, cs.count, total)
    println(toReturn)
    total += cs.count
    toReturn
  }
  override def toString = "Offsetter total:"+total
}

class NamesWriter(path: String) extends IndexWriter[IdName](path, "names") {
  def apply(p: IdName) {
    println(p)
    dstFile.write(s"${p.id}\t${p.name}\n")
  }
}
class LengthsWriter(path: String) extends IndexWriter[IdLength](path, "lengths") {
  def apply(p: IdLength) {
    dstFile.write(s"${p.id}\t${p.length}\n")
  }
}
class PostingsWriter(path: String) extends IndexWriter[Posting](path, "postings") {
  def apply(p: Posting) {
    val str = s"${p.term}\t${p.doc}\t${p.positions.mkString(",")}\n"
    dstFile.write(str)
  }
}



object BuildIndex {
  val offset = new Offsetter
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
    println("intoDocuments "+of+" => Seq[Doc] size="+numbered.size)
    numbered.toSeq
  }

  def tokenize(d: Doc): Doc = {
    d.copy(text = """\s""".r.split(d.content))
  }

  def normalize(d: Doc): Doc = {
    d.copy(text = d.text.map(_.toLowerCase))
  }
  
  def getPostings(d: Doc) = {
    println("getPostings "+d.id)
    val termPositions = d.text.zipWithIndex.groupBy(_._1)
    val postings = termPositions.keys.map { term =>
      val pos = termPositions(term).map(_._2).sorted.toArray
      Posting(term, d.id, pos)
    }
    postings
  }
    
  def namesWriter(prefix: String) = new NamesWriter(prefix)
  def lengthsWriter(prefix: String) = new LengthsWriter(prefix)
  def postingsWriter(prefix: String) = new PostingsWriter(prefix)

  def runForked(files: Seq[String], dest: String) = {
    implicit val encoding = JavaObjectProtocol()
    val jobDispatch = new JobDispatcher
    val cfg = new RuntimeConfig("buildIndex")
    val distrib = 2

    Util.mkdir(dest)
    
    // We start with a list of files
    // Fan them out for counting
    //val counted = files.par.map(count)
    val countedFiles = (0 until distrib).map(_ => cfg.nextScratchName()).toSet
    
    jobDispatch.runSync(
      TraversableSource[String](files),
      new MappedNode[String, CountedSplit](new RoundRobinDistribNode[CountedSplit](countedFiles), count),
      "count"
    )

    assert(countedFiles.exists(fp => encoding.getReader[CountedSplit](fp).nonEmpty))

    // OffsetSplitter
    //val shifted = counted.seq.map(offset)
    val shiftedFiles = (0 until distrib).map(_ => cfg.nextScratchName()).toSet
    jobDispatch.runSync(
      MergedFileSource[CountedSplit](countedFiles),
      new MappedNode[CountedSplit, OffsetSplit](new RoundRobinDistribNode[OffsetSplit](shiftedFiles), offset),
      "shift"
    )
    assert(shiftedFiles.exists(fp => encoding.getReader[OffsetSplit](fp).nonEmpty))

    // Three transformations in a row!
    // ParserSelector/UniversalParser & TagTokenizer & A lemmatizer
    val inputNames = shiftedFiles.toArray
    val outputNames = inputNames.map(_ => cfg.nextScratchName()).toArray
    val lengthsPipes = outputNames.map(_+"len").toArray
    val namesPipes = outputNames.map(_+"names").toArray
    val postingsPipes = outputNames.map(_+"postings").toArray

    println(postingsPipes.mkString(", "))
    
    println(shiftedFiles.zip(outputNames))
    val jobs = shiftedFiles.zip(outputNames).zipWithIndex.map {
      case ((fileInput, nameBase),idx) => {
        // build leaves first
        val multiStep = {
          val docNums = new MappedNode[Doc,Int](new EchoNode[Int](nameBase, new NullNode[Int]()), doc => doc.id)
          val genLengths = new MappedNode[Doc,IdLength](new SortedNode[IdLength](new FileNode[IdLength](nameBase+"len")),
            doc => IdLength(doc.id, doc.text.length)
          )
          val genNames = new MappedNode[Doc,IdName](new SortedNode[IdName](new FileNode[IdName](nameBase+"names")),
            doc => IdName(doc.id, doc.name)
          )
          val genPostings = new FlatMappedNode[Doc,Posting](new SortedNode[Posting](new FileNode[Posting](nameBase+"postings")),
            doc => getPostings(doc)
          )
          println("Input from: "+fileInput)
          println("Output postings to "+nameBase+"postings")
          println("Output names to "+nameBase+"names")
          println("Output lengths to "+nameBase+"names")
          new MultiNode(Seq(docNums, genLengths, genNames, genPostings))
        }

        val parser = {
          val normalizer = new MappedNode[Doc,Doc](multiStep, normalize)
          val tokenizer = new MappedNode[Doc,Doc](normalizer, tokenize)
          val dmapper = new FlatMappedNode[OffsetSplit,Doc](tokenizer, intoDocuments)
          dmapper
        }

        /*
        Executor(
          FileSource[OffsetSplit](fileInput),
          parser
        ).run(new RuntimeConfig("parse"))
        */
        jobDispatch.run(
          FileSource[OffsetSplit](fileInput),
          parser,
          "parse"+idx
        )
      }
    }
    jobDispatch.awaitMany(jobs.toSet)

    val writeLengths = lengthsWriter(dest)
    val lengthsJob = jobDispatch.run(
      new SortedMergeSource[IdLength](lengthsPipes.toSet),
      new ForeachedNode[IdLength, Unit](writeLengths),
      "lengths"
    )
    val writeNames = namesWriter(dest)
    val namesJob = jobDispatch.run(
      new SortedMergeSource[IdName](namesPipes.toSet),
      new ForeachedNode[IdName, Unit](writeNames),
      "names"
    )
    val writePostings = postingsWriter(dest)
    val postingsJob = jobDispatch.run(
      new SortedMergeSource[Posting](postingsPipes.toSet),
      new ForeachedNode[Posting, Unit](writePostings),
      "postings"
    )

    jobDispatch.awaitMany(Set(lengthsJob, namesJob, postingsJob))


    val postingsToMerge = postingsPipes.map(encoding.getReader[Posting](_).map(p => (p.doc)).toSet.toArray.sorted)

    postingsToMerge.foreach { arr =>
      println(arr.mkString("Post: ",",",""))
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
    testSerializable(offset)(JavaObjectProtocol())
    testSerializable(new LengthsWriter("/tmp/"))(JavaObjectProtocol())
    //runLocally(Seq("data/test1","data/test2"), "locally-built-index/")
    runForked(Seq("data/test1","data/test2"), "fork-built-index/")
  }
}

