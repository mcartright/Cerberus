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

   def collectFirst
   def contains
   def count
   def distinct
   def forall
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
   def dropRight
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
   def takeRight
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
  def collect[B](pf: PartialFunction[A, B]): Flow[B]
  def drop(n: Int): Flow[A]
  def dropWhile(p: A => Boolean): Flow[A]
  def take(n: Int): Flow[A]
  def takeWhile(p: A => Boolean): Flow[A]
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
  def collect[B](pf: PartialFunction[A, B]): Flow[B] =
    replace(Collected(this, pf))
  def drop(n: Int): Flow[A] = this.seq.drop(n)
  def dropWhile(p: A => Boolean): Flow[A] = this.seq.dropWhile(p)
  def map[B](f: A => B): Flow[B] = replace(Mapped(this, f))
  def flatMap[B](f: A => GenTraversableOnce[B]): Flow[B] =
    replace(FlatMapped(this, f))
  def filter(p: A => Boolean): Flow[A] = replace(Filtered(this, p))
  def seq: Flow[A] = replace(new Sequential[A] { val incoming = this })
  def par: Flow[A] = this
  def foreach(f: A => Unit): Unit = {
    val child = Foreached(this, f)
    children.append(child)
  }

  // For now, these convert to sequential then perform the operation
  def sorted[B >: A](implicit ord: Ordering[B]): Flow[A] = this.seq.sorted(ord)
  def take(n: Int): Flow[A] = this.seq.take(n)
  def takeWhile(p: A => Boolean): Flow[A] = this.seq.takeWhile(p)

  // the implementing classes
  case class Collected[B](val incoming: Flow[A], val pf: PartialFunction[A, B])
       extends Distributed[B]

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
  def collect[B](pf: PartialFunction[A, B]): Flow[B] =
    replace(Collected(this, pf))
  def drop(n: Int): Flow[A] = replace(Dropped(this, n))
  def dropWhile(p: A => Boolean): Flow[A] = replace(DroppedWhile(this, p))
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

  def take(n: Int): Flow[A] = replace(Taken(this, n))
  def takeWhile(p: A => Boolean): Flow[A] = replace(TakenWhile(this, p))

  // the implementing classes
  case class Dropped(val incoming: Flow[A], val n: Int)
       extends Sequential[A]

  case class DroppedWhile(val incoming: Flow[A], val p: A => Boolean)
       extends Sequential[A]

  case class Collected[B](val incoming: Flow[A], val pf: PartialFunction[A, B])
       extends Sequential[B]

  case class TakenWhile(val incoming: Flow[A], val p: A => Boolean)
  extends Sequential[A]

  case class Taken(val incoming: Flow[A], val n: Int)
       extends Sequential[A]

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

