/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
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
