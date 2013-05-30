package cerberus

import cerberus.io.Encodable
import collection.{GenTraversableOnce, Seq, SeqProxy}
import math.Ordering


/** Entry point for flows.
  */
object Flow {
  val DefaultDistrib = 10
}

abstract class Flow[A <:Encodable]() {
  protected val children = scala.collection.mutable.ListBuffer[Flow[_ <:Encodable]]()
  protected def returnAsChild[B <:Encodable](newChild: Flow[B]) = {
    children += newChild
    newChild
  }
  def hasChildren = children.size > 0
  // automagically named outputs
  protected val outputs = scala.collection.mutable.ListBuffer[String]()

  //def getOutput
  
  // override me
  def hasParent: Boolean
  def parent: Flow[_]

  /** 
   * defined in terms of parent if possible
   * this is overridden in SplitFlow and MergedFlow
   */
  def numSplits: Int = parent.numSplits

  final def isParallel: Boolean = (numSplits > 1)
  final def isSequential: Boolean = numSplits == 1

  def split(n: Int) = {
    if(n == this.numSplits) { 
      this
    } else {
      returnAsChild(new SplitFlow(this, n))
    }
  }
  def par(n: Int=10) = split(n)
  def seq = {
    if(isSequential) {
      this
    } else {
    returnAsChild(new MergedFlow(this))
    }
  }
  def map[B <:Encodable](f: A=>B) = returnAsChild(new MappedFlow(this, f))
  //def sorted[B >: A](implicit ord: Ordering[B]): Flow[A]
}

abstract class BaseFlow[A <:Encodable] extends Flow[A] {
  def contents: GenTraversableOnce[A]
  def hasParent = false
  def parent = ??? //TODO exception type
  override def numSplits = 1
}
class OperFlow[A <:Encodable](val source: Flow[_]) extends Flow[A] {
  def hasParent = true
  def parent = source
}

case class SplitFlow[A <:Encodable](src: Flow[A], val distrib: Int) extends OperFlow[A](src) {
  assert(distrib > 1)
  override def numSplits = distrib // Since this node changes the number of splits, we override these
}
case class MergedFlow[A <:Encodable](src: Flow[A]) extends OperFlow[A](src) {
  override def numSplits = 1 // Since this node changes the number of splits, we override this
}

case class MappedFlow[A <:Encodable, B <:Encodable](src: Flow[A], val oper: A=>B) extends OperFlow[B](src)

case class ForeachedFlow[A <:Encodable](src: Flow[A], val oper: A=>Unit) extends OperFlow[A](src) // Really OperFlow[Unit] but not being picky

case class FlatMappedFlow[A <:Encodable, B <:Encodable](src: Flow[A], val oper: A=>GenTraversableOnce[B]) extends OperFlow[B](src)

case class FilteredFlow[A <:Encodable](src: Flow[A], val pred: A=>Boolean) extends OperFlow[A](src)

case class TraversableFlow[A <:Encodable](data: GenTraversableOnce[A]) extends BaseFlow[A] {
  def contents = data
}

/*
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
*/

