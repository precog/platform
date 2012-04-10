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

trait F0[@specialized(Boolean, Int, Long, Float, Double) A] extends Returning[A] { outer =>
  def apply(row: Int): A

  def andThen[@specialized(Boolean, Int, Long, Float, Double) B](f: F1[_, B]): F0[B] = new F0[B] {
    val returns = f.returns
    def apply(row: Int): B = if (outer.returns == f.accepts) {
      outer.returns match {
        case v @ CBoolean          => v.cast1(f) { v.cast0(outer)(row) } 
        case v @ CStringFixed(_)   => v.cast1(f) { v.cast0(outer)(row) }
        case v @ CStringArbitrary  => v.cast1(f) { v.cast0(outer)(row) }
        case v @ CInt              => v.cast1(f) { v.cast0(outer)(row) }
        case v @ CLong             => v.cast1(f) { v.cast0(outer)(row) }
        case v @ CFloat            => v.cast1(f) { v.cast0(outer)(row) }
        case v @ CDouble           => v.cast1(f) { v.cast0(outer)(row) }
        case v @ CDecimalArbitrary => v.cast1(f) { v.cast0(outer)(row) }
        case v @ CNull             => v.cast1(f) { v.cast0(outer)(row) }
        case v @ CEmptyArray       => v.cast1(f) { v.cast0(outer)(row) }
        case v @ CEmptyObject      => v.cast1(f) { v.cast0(outer)(row) }
      }
    } else {
      sys.error("Argument type mismatch: required " + f.accepts + ", found " + outer.returns)
    }
  }
}

trait F1[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B] extends Returning[B] { outer =>
  def accepts: CType { type CA = A }
  def apply(a: F0[A]): F0[B]

  def compose[@specialized(Boolean, Int, Long, Float, Double) C](f: F1[C, A]): F1[C, B] = new F1[C, B] { 
    val accepts = f.accepts
    val returns = outer.returns
    def apply(c: F0[C]): F0[B] = (c andThen f) andThen outer
  }
  
  def andThen[@specialized(Boolean, Int, Long, Float, Double) C](f: F1[B, C]): F1[A, C] = new F1[A, C] { 
    val accepts = outer.accepts
    val returns = f.returns
    def apply(a: F0[A]): F0[B] = (a andThen outer) andThen f
  }
}

trait F2[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B, @specialized(Boolean, Int, Long, Float, Double) C] extends Returning[C] { outer =>
  def accepts: (CType { type CA = A }, CType { type CA = B })
  def apply(a: F0[A], b: F0[B]): F0[C]

  def andThen[@specialized(Boolean, Int, Long, Float, Double) D](f: F1[_, D]) = new F2[A, B, D] {
    val accepts = self.accepts
    val returns = f.returns
    def apply(a: F0[A], b: F0[B]): F0[D] = self(a, b) andThen f
  }
}

// vim: set ts=4 sw=4 et:
