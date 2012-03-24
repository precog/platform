package com.precog.yggdrasil

import scala.annotation.tailrec
import com.precog.common.VectorCase

case class IterableDataset[A](idCount: Int, iterable: Iterable[(Identities, A)]) extends Iterable[A] {
  def iterator: Iterator[A] = iterable.map(_._2).iterator

  def padIdsTo(width: Int, nextId: => Long): IterableDataset[A] = {
    @tailrec def padded(padTo: Int, ids: VectorCase[Long]): VectorCase[Long] = if (padTo <= 0) ids else padded(padTo - 1, ids :+ nextId)

    IterableDataset(width, iterable map { case (ids, a) => (padded(idCount - width, ids), a) })
  }
}
