package cerberus

import scala.collection.GenTraversableOnce

// Everything should look like this externally
sealed trait AbstractFlow[+A]

trait Distributed[A] extends AbstractFlow[A] {

  // Methods
  def map[B](f: A => B): AbstractFlow[B] = Mapped(this, f)
  def flatMap[B](f: A => GenTraversableOnce[B]): AbstractFlow[B] =
    FlatMapped(this, f)
  def filter(p: A => Boolean): AbstractFlow[A] = Filtered(this, p)
  def seq: AbstractFlow[A] = new Sequential[A] {}
  def par: AbstractFlow[A] = this

  // the implementing classes
  case class Mapped[B](val incoming: AbstractFlow[A], val f: A => B)
      extends Distributed[B]

  case class FlatMapped[B](
    val incoming: AbstractFlow[A],
    val f: A => GenTraversableOnce[B]
  ) extends Distributed[B]

  case class Filtered[A](
    val incoming: AbstractFlow[A],
    val p: A => Boolean
  ) extends Distributed[A]
}

trait Sequential[A] extends AbstractFlow[A] {

  // Methods
  def map[B](f: A => B): AbstractFlow[B] = Mapped(this, f)
  def flatMap[B](f: A => GenTraversableOnce[B]): AbstractFlow[B] =
    FlatMapped(this, f)
  def filter(p: A => Boolean): AbstractFlow[A] = Filtered(this, p)
  def par: AbstractFlow[A] = new Distributed[A] {}
  def seq: AbstractFlow[A] = this

  // the implementing classes
  case class Mapped[B](val incoming: AbstractFlow[A], val f: A => B)
      extends Sequential[B]

  case class FlatMapped[B](
    val incoming: AbstractFlow[A],
    val f: A => GenTraversableOnce[B]
  ) extends Sequential[B]

  case class Filtered[A](
    val incoming: AbstractFlow[A],
    val p: A => Boolean
  ) extends Sequential[A]
}

