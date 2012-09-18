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
package com.precog

import scalaz.Order
import scalaz.Monoid
import java.util.Comparator

package object util {
  class Order2JComparator[A](order: Order[A]) {
    def toJavaComparator: Comparator[A] = new Comparator[A] {
      def compare(a1: A, a2: A) = {
        order.order(a1, a2).toInt
      }
    }
  }

  implicit def Order2JComparator[A](order: Order[A]): Order2JComparator[A] = new Order2JComparator(order)

  def using[A, B](a: A)(f: A => B)(implicit close: Close[A]): B = {
    val result = f(a)
    close.close(a)
    result
  }

  private val MAX_LONG = BigInt(Long.MaxValue)
  private val MIN_LONG = BigInt(Long.MinValue)
  
  @inline
  final def isValidLong(i: BigInt): Boolean = {
    MIN_LONG <= i && i <= MAX_LONG
  }

  implicit def vectorMonoid[A]: Monoid[Vector[A]] = new Monoid[Vector[A]] {
    def zero: Vector[A] = Vector.empty[A]
    def append(v1: Vector[A], v2: => Vector[A]) = v1 ++ v2
  }

  implicit def bigDecimalMonoid: Monoid[BigDecimal] = new Monoid[BigDecimal] {
    def zero: BigDecimal = BigDecimal(0)
    def append(v1: BigDecimal, v2: => BigDecimal): BigDecimal = v1 + v2
  }


  sealed trait Kestrel[A] {
    protected def a: A
    def tap(f: A => Unit): A = { f(a); a }
  }

  implicit def any2Kestrel[A](a0: => A): Kestrel[A] = new Kestrel[A] {
    lazy val a = a0
  }
}

// vim: set ts=4 sw=4 et:
