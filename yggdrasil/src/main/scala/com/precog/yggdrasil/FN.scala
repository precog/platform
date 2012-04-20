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

trait Returning[@specialized(Boolean, Int, Long, Float, Double) A] { 
  def returns: CType { type CA = A }
}

trait F1[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B] extends Returning[B] { outer =>
  def accepts: CType { type CA = A }

  def isDefinedAt(a: Column[A]): Int => Boolean

  def apply(a: Column[A]): Column[B] 

  def compose[@specialized(Boolean, Int, Long, Float, Double) C](f: F1[C, A]): F1[C, B] = new F1[C, B] { 
    val accepts = f.accepts
    val returns = outer.returns
    def isDefinedAt(c: Column[C]) = f andThen outer isDefinedAt c
    def apply(c: Column[C]): Column[B] = c |> f |> outer
  }
  
  def andThen[@specialized(Boolean, Int, Long, Float, Double) C](f: F1[B, C]): F1[A, C] = new F1[A, C] { 
    val accepts = outer.accepts
    val returns = f.returns
    def isDefinedAt(a: Column[A]) = outer andThen f isDefinedAt a
    def apply(a: Column[A]): Column[C] = a |> outer |> f
  }
}

trait F2[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B, @specialized(Boolean, Int, Long, Float, Double) C] extends Returning[C] { outer =>
  import F2._
  def accepts: (CType { type CA = A }, CType { type CA = B })

  def isDefinedAt(a: Column[A], b: Column[B]): Int => Boolean

  def apply(a: Column[A], b: Column[B]): Column[C]

  def andThen[@specialized(Boolean, Int, Long, Float, Double) D](f: F1[C, D]) = new F2[A, B, D] {
    val accepts = outer.accepts
    val returns = f.returns

    def isDefinedAt(a: Column[A], b: Column[B]): Int => Boolean = (row: Int) => outer(a, b).isDefinedAt(row) && f.isDefinedAt(outer(a, b))(row)
    def apply(a: Column[A], b: Column[B]): Column[D] = (new Col[A, B, C](a, b, outer) /* with MemoizingColumn[C] */) |> f
  }
}

object F2 {
  //TODO: Minimize the scalac bug...
  private class Col[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B, @specialized(Boolean, Int, Long, Float, Double) C](a: Column[A], b: Column[B], f2: F2[A, B, C]) extends Column[C] {
    val returns = f2.returns
    def isDefinedAt(row: Int): Boolean = f2.isDefinedAt(a, b)(row)
    def apply(row: Int): C = f2(a, b)(row)
  }
}

// Pure functions that can be promoted to FNs.

trait F1P[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B] extends Returning[B] { outer =>
  import F1P._

  def accepts: CType { type CA = A }
  def isDefinedAt(a: A): Boolean
  def apply(a: A): B

  @inline final def applyCast(a: Any): B = apply(accepts.cast(a))

  final def compose[@specialized(Boolean, Int, Long, Float, Double) C](f: F1P[C, A]): F1P[C, B] =  {
    new F1P[C, B] {
      private var _arg: C = _
      private var _value: B = _

      val accepts = f.accepts
      val returns = outer.returns
      def isDefinedAt(c: C): Boolean = outer.isDefinedAt(f(c))
      def apply(c: C) = {
        if (_arg != c) {
          _arg = c
          _value = outer(f(c))
        }

        _value
      }
    }
  }

  final def andThen[@specialized(Boolean, Int, Long, Float, Double) C](f: F1P[B, C]): F1P[A, C] = {
    new F1P[A, C] {
      private var _arg: A = _
      private var _value: C = _

      val accepts = outer.accepts
      val returns = f.returns
      def isDefinedAt(a: A) = f.isDefinedAt(outer(a))
      def apply(a: A) = {
        if (_arg != a) {
          _arg = a
          _value = f(outer(a))
        }

        _value
      }
    }
  }

  final def toF1: F1[A, B] = {
    new F1[A, B] {
      private var _ca: Column[A] = _
      private var _value: Column[B] = _

      val accepts = outer.accepts
      val returns = outer.returns

      def isDefinedAt(ca: Column[A]): Int => Boolean = 
        apply(ca).isDefinedAt _

      def apply(ca: Column[A]): Column[B] = {
        if (ca != _ca) {
          _ca = ca
          _value = new Col[A, B](_ca, outer) //with MemoizingColumn[B]
        }

        _value
      }
    }
  }
}

object F1P {
  private class Col[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B](ca: Column[A], f: F1P[A, B]) extends Column[B] {
    val returns = f.returns
    def isDefinedAt(row: Int) = ca.isDefinedAt(row) && f.isDefinedAt(ca(row))
    def apply(row: Int) = f(ca(row))
  }
}

trait F2P[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B, @specialized(Boolean, Int, Long, Float, Double) C] extends Returning[C] { outer =>
  import F2P._
  def accepts: (CType { type CA = A }, CType { type CA = B })
  def isDefinedAt(a: A, b: B): Boolean
  def apply(a: A, b: B): C

  @inline final def applyCast(a: Any, b: Any): C = apply(accepts._1.cast(a), accepts._2.cast(b))

  final def andThen[@specialized(Boolean, Int, Long, Float, Double) D](f: F1P[C, D]): F2P[A, B, D] = {
    new F2P[A, B, D] {
      val accepts = outer.accepts
      val returns = f.returns
      def isDefinedAt(a: A, b: B) = outer.isDefinedAt(a, b) && f.isDefinedAt(outer(a, b))
      def apply(a: A, b: B): D = f(outer(a, b))
    }
  }

  final def toF2: F2[A, B, C] = {
    new F2[A, B, C] {
      private var _ca: Column[A] = _
      private var _cb: Column[B] = _
      private var _value: Column[C] = _

      val accepts = outer.accepts
      val returns = outer.returns

      def isDefinedAt(ca: Column[A], cb: Column[B]): Int => Boolean = 
        apply(ca, cb).isDefinedAt _

      def apply(ca: Column[A], cb: Column[B]): Column[C] = {
        if ((ca != _ca) || (cb != _cb)) {
          _ca = ca
          _cb = cb
          _value = new Col[A, B, C](ca, cb, outer) //with MemoizingColumn[C]
        }

        _value
      }
    }
  }
}

object F2P {
  private class Col[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B, @specialized(Boolean, Int, Long, Float, Double) C](ca: Column[A], cb: Column[B], f: F2P[A, B, C]) extends Column[C] {
    val returns = f.returns
    def isDefinedAt(row: Int) = ca.isDefinedAt(row) && cb.isDefinedAt(row) && f.isDefinedAt(ca(row), cb(row))
    def apply(row: Int) = f.apply(ca(row), cb(row))
  }
}

