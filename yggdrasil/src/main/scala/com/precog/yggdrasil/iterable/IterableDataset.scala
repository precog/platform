package com.precog.yggdrasil
package iterable

import com.precog.common.VectorCase

import scala.annotation.tailrec
import scalaz.Functor

case class IterableDataset[A](idCount: Int, iterable: Iterable[(Identities, A)]) {
  def iterator: Iterator[A] = iterable.map(_._2).iterator

  def map[B](f: A => B): IterableDataset[B] = IterableDataset(idCount, iterable.map { case (i, v) => (i, f(v)) })

  def padIdsTo(width: Int, nextId: => Long): IterableDataset[A] = {
    @tailrec def padded(padTo: Int, ids: VectorCase[Long]): VectorCase[Long] = 
      if (padTo <= 0) ids else padded(padTo - 1, ids :+ nextId)

    IterableDataset(width, iterable map { case (ids, a) => (padded(width - idCount, ids), a) })
  }
}

object IterableDataset {
  implicit object functor extends Functor[IterableDataset] {
    def map[A, B](d: IterableDataset[A])(f: A => B): IterableDataset[B] = d.map(f)
  }
}
