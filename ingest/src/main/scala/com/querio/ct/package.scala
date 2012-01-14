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
package com.querio

import scala.collection.immutable.SortedMap
import scalaz.Scalaz._
import scalaz.Semigroup

package object ct {
  implicit def mActions[M[_], A](m: M[A]): Actions[M, A] = new Actions[M, A] {
    override val value = m
  }

  implicit def pf[A](a: A): PF[A] = new PF(a)
  implicit def SortedMapSemigroup[K, V](implicit ss: Semigroup[V]): Semigroup[SortedMap[K, V]] = new Semigroup[SortedMap[K, V]] {
    def append(m1: SortedMap[K, V], m2: => SortedMap[K, V]) = {
      // semigroups are not commutative, so order may matter. 
      val (from, to, semigroup) = {
        if (m1.size > m2.size) (m2, m1, ss.append(_: V, _: V))
        else (m1, m2, (ss.append(_: V, _: V)).flip)
      }

      from.foldLeft(to) {
        case (to, (k, v)) => to + (k -> to.get(k).map(semigroup(_, v)).getOrElse(v))
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
