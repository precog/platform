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

  def apply(a: Column[A]): Column[B] 

  def compose[@specialized(Boolean, Int, Long, Float, Double) C](f: F1[C, A]): F1[C, B] = new F1[C, B] { 
    val accepts = f.accepts
    val returns = outer.returns
    def apply(c: Column[C]): Column[B] = c |> f |> outer
  }
  
  def andThen[@specialized(Boolean, Int, Long, Float, Double) C](f: F1[B, C]): F1[A, C] = new F1[A, C] { 
    val accepts = outer.accepts
    val returns = f.returns
    def apply(a: Column[A]): Column[C] = a |> outer |> f
  }
}

trait F2[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B, @specialized(Boolean, Int, Long, Float, Double) C] extends Returning[C] { outer =>
  def accepts: (CType { type CA = A }, CType { type CA = B })

  def apply(a: Column[A], b: Column[B]): Column[C]

  def andThen[@specialized(Boolean, Int, Long, Float, Double) D](f: F1[C, D]) = new F2[A, B, D] {
    val accepts = outer.accepts
    val returns = f.returns

    def apply(a: Column[A], b: Column[B]): Column[D] = outer(a, b) |> f
  }
}

// Pure functions that can be promoted to FNs.

trait F1P[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B] extends Returning[B] { outer =>
  def accepts: CType { type CA = A }
  def isDefinedAt(a: A): Boolean
  def apply(a: A): B

  @inline final def applyCast(a: Any): B = apply(accepts.cast(a))

  final def compose[@specialized(Boolean, Int, Long, Float, Double) C](f: F1P[C, A]): F1P[C, B] =  {
    new F1P[C, B] {
      private var _c: C = _
      private var _b: B = _

      val accepts = f.accepts
      val returns = outer.returns
      def isDefinedAt(c: C): Boolean = outer.isDefinedAt(f(c))
      def apply(c: C) = {
        if (_c != c) {
          _c = c
          _b = outer(f(c))
        }

        _b
      }
    }
  }

  final def andThen[@specialized(Boolean, Int, Long, Float, Double) C](f: F1P[B, C]): F1P[A, C] = {
    new F1P[A, C] {
      private var _a: A = _
      private var _c: C = _

      val accepts = outer.accepts
      val returns = f.returns
      def isDefinedAt(a: A) = f.isDefinedAt(outer(a))
      def apply(a: A) = {
        if (_a != a) {
          _a = a
          _c = f(outer(a))
        }

        _c
      }
    }
  }

  final def toF1: F1[A, B] = {
    new F1[A, B] {
      val accepts = outer.accepts
      val returns = outer.returns

      def apply(ca: Column[A]): Column[B] = new Column[B] {
        val returns = outer.returns
        def isDefinedAt(row: Int) = ca.isDefinedAt(row) && outer.isDefinedAt(ca(row))
        def apply(row: Int) = outer(ca(row))
      }
    }
  }
}

trait F2P[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B, @specialized(Boolean, Int, Long, Float, Double) C] extends Returning[C] { outer =>
  def accepts: (CType { type CA = A }, CType { type CA = B })
  def isDefinedAt(a: A, b: B): Boolean
  def apply(a: A, b: B): C

  @inline final def applyCast(a: Any, b: Any): C = apply(accepts._1.cast(a), accepts._2.cast(b))

  final def andThen[@specialized(Boolean, Int, Long, Float, Double) D](f: F1P[C, D]): F2P[A, B, D] = {
    new F2P[A, B, D] {
      private var _a: A = _
      private var _b: B = _
      private var _d: D = _

      val accepts = outer.accepts
      val returns = f.returns
      def isDefinedAt(a: A, b: B) = outer.isDefinedAt(a, b) && f.isDefinedAt(outer(a, b))
      def apply(a: A, b: B): D = {
        if (_a != a || _b != b) {
          _a = a
          _b = b
          _d = f(outer(_a, _b))
        }

        _d
      }
    }
  }

  final def toF2: F2[A, B, C] = {
    new F2[A, B, C] {
      val accepts = outer.accepts
      val returns = outer.returns
      def apply(ca: Column[A], cb: Column[B]): Column[C] = new Column[C] {
        val returns = outer.returns
        def isDefinedAt(row: Int) = ca.isDefinedAt(row) && cb.isDefinedAt(row) && outer.isDefinedAt(ca(row), cb(row))
        def apply(row: Int) = outer(ca(row), cb(row))
      }
    }
  }
}
