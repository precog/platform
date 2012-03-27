package com.precog

import scalaz.Order
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
}


// vim: set ts=4 sw=4 et:
