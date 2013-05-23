// We start with a list of files
// DocumentSource
val files = Seq.empty[String]

// Fan them out for counting
// ParserCounter
val counted = files.par(files.size).map { f =>
  val num = count(f)
  (f, num)
}

// OffsetSplitter
// Shift -- NEED STATE HERE...HMM...
// Could do a tail-recursive function (no state needed)
val total = 0
val shifted = counted.seq.foreach { s =>
  // This function is gross
  val toReturn (s_.1, s._2, total)
  total += s._2
  toReturn
}

// Three transformations in a row!
// ParserSelector/UniversalParser & TagTokenizer & A lemmatizer
val parsedDocuments =
  shifted.par.flatMap(intoDocuments).map(tokenize).map(normalize)

// Each document is processed multiple times
// hmm - this should not produce a stream of tuples,
// it needs to produce 1 tuple of 4 streams 0 let's pretend it does that
// val results = parsedDocuments.map { doc =>
//  (getLengths(doc), getName(doc), getPostings(doc), getFieldPostings(doc))
// }

// ok so that's way hard, how about this:
// lengths
parsedDocuments.map {
  doc => (doc.id, doc.length)
}.sortBy(_._1).foreach(pair => write(pair))

// names
parsedDocuments.map {
  doc => (doc.id, doc.name)
}.sortBy(_._1).foreach(pair => write(pair))

// postings
parsedDocuments.flatMap {
  doc => getPostings(doc)
}.sorted.foreach(posting => write(posting))

// field postings
parsedDocuments.flatMap {
  doc => getFieldPostings(doc)
}.sorted.foreach(fp => write(fp))

// corpus
parsedDocuments.sorted.foreach(doc => write(doc))