package cerberus

import collection.{GenTraversableOnce, Seq, SeqProxy}
import math.Ordering

/** Entry point for flows.
  */
object Flow {
  def par[A](gen: GenTraversableOnce[A]): Flow[A] =
    new Distributed[A] with Base[A] { val src = gen }
  def seq[A](gen: GenTraversableOnce[A]): Flow[A] =
    new Sequential[A] with Base[A] { val src = gen }
  def local[A](gen: GenTraversableOnce[A]): Seq[A] = gen.seq.toSeq
}

trait Flow[A] {
  protected val children = scala.collection.mutable.ListBuffer[Flow[_]]()
  protected def replace[B](f : Flow[B]) : Flow[B] = {
    this.children += f
    f
  }


  /* Methods that we would like to implement -- ordered by need/feasibility
   * LHF
   def collect
   def collectFirst
   def contains
   def count
   def distinct
   def forall
   def exists
   def find
   def isDefinedAt
   def isEmpty
   def lengthCompare
   def max
   def maxBy
   def min
   def minBy
   def nonEmpty
   def fold
   def reduce
   def size
   def sortWith
   def sum

   //  Not-so-low-hanging fruit.
   def apply
   def combinations
   def containsSlice
   def drop
   def dropRight
   def dropWhile
   def endsWith
   def flatten
   def foldLeft
   def foldRight
   def foreach
   def groupBy
   def grouped
   def head
   def headOption
   def indexOf
   def indexOfSlice
   def indexWhere(p, from)
   def indexWhere(p)
   def indices
   def init
   def inits
   def intersect
   def iterator
   def last
   def lastIndexOf(elem, end)
   def lastIndexOf(elem)
   def lastIndexOfSlice(that, end)
   def lastIndexOfSlice(that)
   def lastIndexWhere(p, end)
   def lastIndexWhere(p)
   def lastOption
   def partition
   def patch
   def permutations
   def prefixLength
   def product
   def reduceLeft
   def reduceRight
   def reverse
   def reverseIterator
   def reverseMap
   def sameElements
   def scan
   def scanLeft
   def scanRight
   def segmentLength
   def slice
   def sliding(size, step)
   def sliding(size)
   def sortBy
   def span
   def splitAt
   def startsWith(that, offset)
   def startsWith(that)
   def tail
   def tails
   def take
   def takeRight
   def takeWhile
   def toSeq
   def union
   def zip
   def zipAll
   def zipWithIndex
   */

  def foreach(f: A => Unit): Unit
  def sorted[B >: A](implicit ord: Ordering[B]): Flow[A]
  def map[B](f: A => B): Flow[B]
  def flatMap[B](f: A => GenTraversableOnce[B]): Flow[B]
  def filter(p: A => Boolean): Flow[A]
  def filterNot(p: A => Boolean): Flow[A] = filter(!p(_))
  def seq: Flow[A]
  def par: Flow[A]
}

/** Represents the entry point into the graph.
  */
trait Base[A] {
  def src: GenTraversableOnce[A]
}

// Represents distributed computation - essentially parallel computation
// but over machines instead of cores.
trait Distributed[A] extends Flow[A] {

  // Methods
  def map[B](f: A => B): Flow[B] = replace(Mapped(this, f))
  def flatMap[B](f: A => GenTraversableOnce[B]): Flow[B] =
    replace(FlatMapped(this, f))
  def filter(p: A => Boolean): Flow[A] = replace(Filtered(this, p))
  def seq: Flow[A] = replace(new Sequential[A] { val incoming = this })
  def par: Flow[A] = this
  def sorted[B >: A](implicit ord: Ordering[B]): Flow[A] =
    replace(this.seq.sorted(ord))

  def foreach(f: A => Unit): Unit = {
    val child = Foreached(this, f)
    children.append(child)
  }

  // the implementing classes
  case class Foreached(val incoming: Flow[A], val f: A => Unit)
       extends Distributed[A]

  case class Mapped[B](val incoming: Flow[A], val f: A => B)
      extends Distributed[B]

  case class FlatMapped[B](
    val incoming: Flow[A],
    val f: A => GenTraversableOnce[B]
  ) extends Distributed[B]

  case class Filtered[A](
    val incoming: Flow[A],
    val p: A => Boolean
  ) extends Distributed[A]
}

trait Sequential[A] extends Flow[A] {

  // Methods
  def map[B](f: A => B): Flow[B] = replace(Mapped(this, f))
  def flatMap[B](f: A => GenTraversableOnce[B]): Flow[B] =
    replace(FlatMapped(this, f))
  def filter(p: A => Boolean): Flow[A] = replace(Filtered(this, p))
  def par: Flow[A] = replace(new Distributed[A] { val incoming = this })
  def sorted[B >: A](implicit ord: Ordering[B]): Flow[A] =
    replace(new Sorted(this, ord))

  def seq: Flow[A] = this

  def foreach(f: A => Unit): Unit = {
    val child = Foreached(this, f)
    children.append(child)
  }

  // the implementing classes
  case class Foreached(val incoming: Flow[A], val f: A => Unit)
       extends Sequential[A]

  case class Mapped[B](val incoming: Flow[A], val f: A => B)
      extends Sequential[B]

  case class Sorted[A, B >: A](val incoming: Flow[A], val ord: Ordering[B])
      extends Sequential[A]

  case class FlatMapped[B](
    val incoming: Flow[A],
    val f: A => GenTraversableOnce[B]
  ) extends Sequential[B]

  case class Filtered[A](
    val incoming: Flow[A],
    val p: A => Boolean
  ) extends Sequential[A]
}

