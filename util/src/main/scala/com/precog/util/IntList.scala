package com.precog.util

import scala.collection._
import scala.collection.mutable.Builder
import scala.collection.generic.CanBuildFrom
import scala.annotation.tailrec

// Once we move to 2.10, we can abstract this to a specialized-list. In 2.9,
// specialization is just too buggy to get it working (tried).

sealed trait IntList extends LinearSeq[Int] with LinearSeqOptimized[Int, IntList] { self =>
  def head: Int
  def tail: IntList

  def ::(head: Int): IntList = IntCons(head, this)

  override def foreach[@specialized B](f: Int => B): Unit = {
    @tailrec def loop(xs: IntList): Unit = xs match {
      case IntCons(h, t) => f(h); loop(t)
      case _ =>
    }
    loop(this)
  }

  override def apply(idx: Int): Int = {
    @tailrec def loop(xs: IntList, row: Int): Int = xs match {
      case IntCons(x, xs0) =>
        if (row == idx) x else loop(xs0, row + 1)
      case IntNil =>
        throw new IndexOutOfBoundsException("%d is larger than the IntList")
    }
    loop(this, 0)
  }

  override def length: Int = {
    @tailrec def loop(xs: IntList, len: Int): Int = xs match {
      case IntCons(x, xs0) => loop(xs0, len + 1)
      case IntNil => len
    }
    loop(this, 0)
  }

  override def iterator: Iterator[Int] = new Iterator[Int] {
    private var xs: IntList = self
    def hasNext: Boolean = xs != IntNil
    def next(): Int = {
      val result = xs.head
      xs = xs.tail
      result
    }
  }

  override def reverse: IntList = {
    @tailrec def loop(xs: IntList, ys: IntList): IntList = xs match {
      case IntCons(x, xs0) => loop(xs0, x :: ys)
      case IntNil => ys
    }
    loop(this, IntNil)
  }

  override protected def newBuilder = new IntListBuilder
}

final case class IntCons(override val head: Int, override val tail: IntList) extends IntList {
  override def isEmpty: Boolean = false
}

final case object IntNil extends IntList {
  override def head: Int = sys.error("no head on empty IntList")
  override def tail: IntList = IntNil
  override def isEmpty: Boolean = true
}

final class IntListBuilder extends Builder[Int, IntList] {
  private var xs: IntList = IntNil
  def +=(x: Int) = { xs = x :: xs; this }
  def clear() { xs = IntNil }
  def result() = xs.reverse
}

object IntList {
  implicit def cbf = new CanBuildFrom[IntList, Int, IntList] {
    def apply(): Builder[Int, IntList] = new IntListBuilder
    def apply(from: IntList): Builder[Int, IntList] = apply()
  }
}
