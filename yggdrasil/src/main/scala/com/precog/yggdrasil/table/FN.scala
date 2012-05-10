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
package table

import org.apache.commons.collections.primitives.ArrayIntList

trait FN[@specialized(Boolean, Long, Double) A] { 
  val returns: CType { type CA = A }
}

trait F1[@specialized(Boolean, Long, Double) A, @specialized(Boolean, Long, Double) B] extends FN[B] { outer =>
  val accepts: CType { type CA = A }

  def apply(a: Column[A]): Column[B] 

  def applyCast(a: Column[_]) = apply(a.asInstanceOf[Column[A]])

  def compose[@specialized(Boolean, Long, Double) C](f: F1[C, A]): F1[C, B] = new F1[C, B] { 
    val accepts = f.accepts
    val returns = outer.returns
    def apply(c: Column[C]): Column[B] = c map f map outer
  }
  
  def andThen[@specialized(Boolean, Long, Double) C](f: F1[B, C]): F1[A, C] = new F1[A, C] { 
    val accepts = outer.accepts
    val returns = f.returns
    def apply(a: Column[A]): Column[C] = a map outer map f
  }
}

trait F2[@specialized(Boolean, Long, Double) A, @specialized(Boolean, Long, Double) B, @specialized(Boolean, Long, Double) C] extends FN[C] { outer =>
  val accepts: (CType { type CA = A }, CType { type CA = B })

  def apply(a: Column[A], b: Column[B]): Column[C]

  def applyCast(a: Column[_], b: Column[_]): Column[C] = 
    apply(a.asInstanceOf[Column[A]], b.asInstanceOf[Column[B]])

  def andThen[@specialized(Boolean, Long, Double) D](f: F1[C, D]) = new F2[A, B, D] {
    val accepts = outer.accepts
    val returns = f.returns

    def apply(a: Column[A], b: Column[B]): Column[D] = outer(a, b) map f
  }
}

object F2 {
  def unify[@specialized(Boolean, Long, Double) A](ctype: CType { type CA = A }): F2[A, A, A] = new F2[A, A, A] {
    val accepts = (ctype, ctype)
    val returns = ctype

    def apply(a: Column[A], b: Column[A]): Column[A] = new Column[A] {
      val returns = ctype
      def isDefinedAt(row: Int) = a.isDefinedAt(row) || b.isDefinedAt(row)
      def apply(row: Int) = if (a.isDefinedAt(row)) {
        if (b.isDefinedAt(row)) b(row) else a(row)
      } else {
        if (b.isDefinedAt(row)) {
          b(row)
        } else {
          throw new IllegalStateException("Attempt to retrieve undefined value for row: " + row)
        }
      }
    }
  }
}

// Pure functions that can be promoted to FNs.

trait F1P[@specialized(Boolean, Long, Double) A, @specialized(Boolean, Long, Double) B] extends FN[B] { outer =>
  def accepts: CType { type CA = A }
  def isDefinedAt(a: A): Boolean
  def apply(a: A): B

  @inline 
  final def applyCast(a: Any): B = apply(accepts.cast(a))

  final def compose[@specialized(Boolean, Long, Double) C](f: F1P[C, A]): F1P[C, B] =  {
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

  final def andThen[@specialized(Boolean, Long, Double) C](f: F1P[B, C]): F1P[A, C] = {
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

  final val toF1: F1[A, B] = {
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

trait TotalF1P[@specialized(Boolean, Long, Double) A, @specialized(Boolean, Long, Double) B] extends F1P[A, B] {
  def isDefinedAt(a: A) = true
}

object F1P {
  def bufferRemap(buf: ArrayIntList): F1P[Int, Int] = new F1P[Int, Int] {
    val accepts, returns = CInt
    def isDefinedAt(i: Int) = (i < buf.size) && buf.get(i) != -1
    def apply(i: Int) = buf.get(i)
  }

  def bufferRemap(buf: Array[Int]): F1P[Int, Int] = new F1P[Int, Int] {
    val accepts, returns = CInt
    def isDefinedAt(i: Int) = (i < buf.length) && buf(i) != -1
    def apply(i: Int) = buf(i)
  }
}

trait F2P[@specialized(Boolean, Long, Double) A, @specialized(Boolean, Long, Double) B, @specialized(Boolean, Long, Double) C] extends FN[C] { outer =>
  def accepts: (CType { type CA = A }, CType { type CA = B })
  def isDefinedAt(a: A, b: B): Boolean
  def apply(a: A, b: B): C

  @inline 
  final def applyCast(a: Any, b: Any): C = apply(accepts._1.cast(a), accepts._2.cast(b))

  final def andThen[@specialized(Boolean, Long, Double) D](f: F1P[C, D]): F2P[A, B, D] = {
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

  final val toF2: F2[A, B, C] = {
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

trait UnaryOpSet {
  def add(f: F1P[_, _]): UnaryOpSet
  def choose[A](argt: CType { type CA = A }): F1P[A, _]
}

trait BinaryOpSet {
  def add(f: F2P[_, _, _]): BinaryOpSet
  def choose[A, B](arg1t: CType { type CA = A }, arg2t: CType { type CA = B }): F2P[A, B, _]
}
