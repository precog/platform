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
/*
 * Copyright (c) 2011, Daniel Spiewak
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * 
 * - Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer. 
 * - Redistributions in binary form must reproduce the above copyright notice, this
 *   list of conditions and the following disclaimer in the documentation and/or
 *   other materials provided with the distribution.
 * - Neither the name of "Anti-XML" nor the names of its contributors may
 *   be used to endorse or promote products derived from this software without
 *   specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.precog.common

import scala.collection.IndexedSeqLike
import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.{IndexedSeq, VectorBuilder}
import scala.collection.mutable.{ArrayBuffer, Builder}
import scala.math.Ordering

private[precog] sealed trait VectorCase[+A] extends IndexedSeq[A] with IndexedSeqLike[A, VectorCase[A]] with Serializable {
  
  override protected[this] def newBuilder: Builder[A, VectorCase[A]] = VectorCase.newBuilder[A]
  
  def +:[B >: A](b: B): VectorCase[B]
  def :+[B >: A](b: B): VectorCase[B]
  
  def apply(index: Int): A
  def updated[B >: A](index: Int, b: B): VectorCase[B]
  
  def ++[B >: A](that: VectorCase[B]): VectorCase[B]
  
  def toVector: Vector[A]
}

private[precog] object VectorCase {
//  implicit def canBuildFrom[A]: CanBuildFrom[Traversable[_], A, VectorCase[A]] = new CanBuildFrom[Traversable[_], A, VectorCase[A]] {
//    def apply() = newBuilder[A]
//    def apply(from: Traversable[_]) = newBuilder[A]
//  }
  
  def empty[A]: VectorCase[A] = Vector0
  
  def newBuilder[A]: Builder[A, VectorCase[A]] = new Builder[A, VectorCase[A]] { this: Builder[A, VectorCase[A]] =>
    val small = new ArrayBuffer[A](4)
    var builder: VectorBuilder[A] = _
    
    def +=(a: A) = {
      if (builder == null) {
        small += a
        
        if (small.length > 4) {
          builder = new VectorBuilder[A]
          builder ++= small
        }
      } else {
        builder += a
      }
      this
    }
    
    override def ++=(seq: TraversableOnce[A]) = {
      if (builder == null) {
        small ++= seq
        
        if (small.length > 4) {
          builder = new VectorBuilder[A]
          builder ++= small
        }
      } else {
        builder ++= seq
      }
      this
    }
    
    def result() = {
      if (builder == null) {
        small.length match {
          case 0 => Vector0
          case 1 => Vector1(small(0))
          case 2 => Vector2(small(0), small(1))
          case 3 => Vector3(small(0), small(1), small(2))
          case 4 => Vector4(small(0), small(1), small(2), small(3))
        }
      } else {
        VectorN(builder.result())
      }
    }
    
    def clear() = this
  }
  
  def apply[A](as: A*) = fromSeq(as)

  def unapplySeq[A](x: VectorCase[A]) : Option[IndexedSeq[A]] = Some(x)
  
  def fromSeq[A](seq: Seq[A]): VectorCase[A] = seq match {
    case c: VectorCase[_] => c
    case _ if seq.lengthCompare(0) <= 0 => Vector0
    case _ if seq.lengthCompare(1) <= 0 => Vector1(seq(0))
    case _ if seq.lengthCompare(2) <= 0 => Vector2(seq(0), seq(1))
    case _ if seq.lengthCompare(3) <= 0 => Vector3(seq(0), seq(1), seq(2))
    case _ if seq.lengthCompare(4) <= 0 => Vector4(seq(0), seq(1), seq(2), seq(3))
    case vec: Vector[_] => VectorN(vec)
    case _ => VectorN(Vector(seq: _*))
  }
}

private[precog] case object Vector0 extends VectorCase[Nothing] {
  def length = 0
  
  def +:[B](b: B) = Vector1(b)
  def :+[B](b: B) = Vector1(b)
  
  def apply(index: Int) = sys.error("Apply on empty vector")
  def updated[B](index: Int, b: B) = sys.error("Updated on empty vector")
  
  def ++[B](that: VectorCase[B]) = that
  
  override def iterator = Iterator.empty
  
  override def foreach[U](f: Nothing => U) {}
  
  def toVector = Vector()
}

private[precog] case class Vector1[+A](_1: A) extends VectorCase[A] {
  def length = 1
  
  def +:[B >: A](b: B) = Vector2(b, _1)
  def :+[B >: A](b: B) = Vector2(_1, b)
  
  def apply(index: Int) = {
    if (index == 0) 
      _1 
    else
      throw new IndexOutOfBoundsException(index.toString)
  }
  
  def updated[B >: A](index: Int, b: B) = {
    if (index == 0)
      Vector1(b)
    else
      throw new IndexOutOfBoundsException(index.toString)
  }
  
  def ++[B >: A](that: VectorCase[B]) = that match {
    case Vector0 => this
    case Vector1(_2) => Vector2(_1, _2)
    case Vector2(_2, _3) => Vector3(_1, _2, _3)
    case Vector3(_2, _3, _4) => Vector4(_1, _2, _3, _4)
    case _: Vector4[_] | _: VectorN[_] =>
      VectorN(_1 +: that.toVector)
  }
  
  override def foreach[U](f: A => U) {
    f(_1)
  }

  // TODO more methods
  
  def toVector = Vector(_1)
}

private[precog] case class Vector2[+A](_1: A, _2: A) extends VectorCase[A] {
  def length = 2
  
  def +:[B >: A](b: B) = Vector3(b, _1, _2)
  def :+[B >: A](b: B) = Vector3(_1, _2, b)
  
  def apply(index: Int) = index match {
    case 0 => _1
    case 1 => _2
    case _ => throw new IndexOutOfBoundsException(index.toString)
  }
  
  def updated[B >: A](index: Int, b: B) = index match {
    case 0 => Vector2(b, _2)
    case 1 => Vector2(_1, b)
    case _ => throw new IndexOutOfBoundsException(index.toString)
  }
  
  def ++[B >: A](that: VectorCase[B]) : VectorCase[B] = that match {
    case Vector0 => this
    case Vector1(_3) => Vector3(_1, _2, _3)
    case Vector2(_3, _4) => Vector4(_1, _2, _3, _4)
    case _: Vector3[_] | _: Vector4[_] | _: VectorN[_] =>
      VectorN(Vector(_1, _2) ++ that.toVector)
  }
  
  override def foreach[U](f: A => U) {
    f(_1)
    f(_2)
  }

  // TODO more methods
  
  def toVector = Vector(_1, _2)
}

private[precog] case class Vector3[+A](_1: A, _2: A, _3: A) extends VectorCase[A] {
  def length = 3
  
  def +:[B >: A](b: B) = Vector4(b, _1, _2, _3)
  def :+[B >: A](b: B) = Vector4(_1, _2, _3, b)
  
  def apply(index: Int) = index match {
    case 0 => _1
    case 1 => _2
    case 2 => _3
    case _ => throw new IndexOutOfBoundsException(index.toString)
  }
  
  def updated[B >: A](index: Int, b: B) = index match {
    case 0 => Vector3(b, _2, _3)
    case 1 => Vector3(_1, b, _3)
    case 2 => Vector3(_1, _2, b)
    case _ => throw new IndexOutOfBoundsException(index.toString)
  }
  
  def ++[B >: A](that: VectorCase[B]) : VectorCase[B] = that match {
    case Vector0 => this
    case Vector1(_4) => Vector4(_1, _2, _3, _4)
    case _: Vector2[_] | _: Vector3[_] | _: Vector4[_] | _: VectorN[_] =>
      VectorN(Vector(_1, _2, _3) ++ that.toVector)
  }
  
  override def foreach[U](f: A => U) {
    f(_1) 
    f(_2)
    f(_3)
  }

  // TODO more methods
  
  def toVector = Vector(_1, _2, _3)
}

private[precog] case class Vector4[+A](_1: A, _2: A, _3: A, _4: A) extends VectorCase[A] {
  def length = 4
  
  def +:[B >: A](b: B) = VectorN(Vector(b, _1, _2, _3, _4))
  def :+[B >: A](b: B) = VectorN(Vector(_1, _2, _3, _4, b))
  
  def apply(index: Int) = index match {
    case 0 => _1
    case 1 => _2
    case 2 => _3
    case 3 => _4
    case _ => throw new IndexOutOfBoundsException(index.toString)
  }
  
  def updated[B >: A](index: Int, b: B) = index match {
    case 0 => Vector4(b, _2, _3, _4)
    case 1 => Vector4(_1, b, _3, _4)
    case 2 => Vector4(_1, _2, b, _4)
    case 3 => Vector4(_1, _2, _3, b)
    case _ => throw new IndexOutOfBoundsException(index.toString)
  }
  
  def ++[B >: A](that: VectorCase[B]): VectorCase[B] = that match {
    case Vector0 => this
    case _: Vector1[_] | _: Vector2[_] | _: Vector3[_] | _: Vector4[_] | _: VectorN[_] =>
      VectorN(Vector(_1, _2, _3, _4) ++ that.toVector)
  }
  
  override def foreach[U](f: A => U) {
    f(_1)
    f(_2)
    f(_3)
    f(_4)
  }

  // TODO more methods
  
  def toVector = Vector(_1, _2, _3, _4)
}

private[precog] case class VectorN[+A](vector: Vector[A]) extends VectorCase[A] {
  def length = vector.length
  
  def +:[B >:A](b: B) = VectorN(b +: vector)
  def :+[B >:A](b: B) = VectorN(vector :+ b)
  
  def apply(index: Int) = vector(index)
  def updated[B >: A](index: Int, b: B) = VectorN(vector.updated(index, b))
  
  def ++[B >: A](that: VectorCase[B]) = VectorN(vector ++ that.toVector)
  
  override def drop(n: Int) = {
    if (n <= 0) {
      this
    } else {
      (vector.length - n) match {
        case x if x <= 0 => Vector0
        case 1 => Vector1(vector(vector.length - 1))
        case 2 => Vector2(vector(vector.length - 2), vector(vector.length - 1))
        case 3 => Vector3(vector(vector.length - 3), vector(vector.length - 2), vector(vector.length - 1))
        case 4 => Vector4(vector(vector.length - 4), vector(vector.length - 3), vector(vector.length - 2), vector(vector.length - 1))
        case _ => VectorN(vector drop n)
      }
    }
  }
  
  override def dropRight(n: Int) = {
    if (n <= 0) {
      this
    } else {
      (vector.length - n) match {
        case x if x <= 0 => Vector0
        case 1 => Vector1(vector(0))
        case 2 => Vector2(vector(0), vector(1))
        case 3 => Vector3(vector(0), vector(1), vector(2))
        case 4 => Vector4(vector(0), vector(1), vector(2), vector(3))
        case _ => VectorN(vector dropRight n)
      }
    }
  }
  
  override def init = (vector.length - 1) match {
    case x if x <= 0 => Vector0
    case 1 => Vector1(vector(0))
    case 2 => Vector2(vector(0), vector(1))
    case 3 => Vector3(vector(0), vector(1), vector(2))
    case 4 => Vector4(vector(0), vector(1), vector(2), vector(3))
    case _ => VectorN(vector.init)
  }
  
  override def slice(from: Int, until: Int) = take(until).drop(from)
  
  override def splitAt(n: Int) = (take(n), drop(n))
  
  override def tail = (vector.length - 1) match {
    case x if x <= 0 => Vector0
    case 1 => Vector1(vector(1))
    case 2 => Vector2(vector(1), vector(2))
    case 3 => Vector3(vector(1), vector(2), vector(3))
    case 4 => Vector4(vector(1), vector(2), vector(3), vector(4))
    case _ => VectorN(vector.tail)
  }
  
  override def take(n: Int) = {
    if (n >= length) {
      this
    } else {
      n match {
        case x if x <= 0 => Vector0
        case 1 => Vector1(vector(0))
        case 2 => Vector2(vector(0), vector(1))
        case 3 => Vector3(vector(0), vector(1), vector(2))
        case 4 => Vector4(vector(0), vector(1), vector(2), vector(3))
        case _ => VectorN(vector take n)
      }
    }
  }
  
  override def takeRight(n: Int) = {
    if (n >= length) {
      this
    } else {
      n match {
        case x if x <= 0 => Vector0
        case 1 => Vector1(vector(vector.length - 1))
        case 2 => Vector2(vector(vector.length - 2), vector(vector.length - 1))
        case 3 => Vector3(vector(vector.length - 3), vector(vector.length - 2), vector(vector.length - 1))
        case 4 => Vector4(vector(vector.length - 4), vector(vector.length - 3), vector(vector.length - 2), vector(vector.length - 1))
        case _ => VectorN(vector takeRight n)
      }
    }
  }
  
  // note: this actually defeats a HotSpot optimization in trivial micro-benchmarks
  override def iterator = vector.iterator
  
  override def foreach[U](f: A => U) {vector.foreach(f)}

  def toVector = vector
}
