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
package com.precog.niflheim

import com.precog.common._
import com.precog.common.json._
import com.precog.util._
import com.precog.util.BitSetUtil.Implicits._

import scala.{ specialized => spec }

import blueeyes.json._

case class SegmentId(blockid: Long, cpath: CPath, ctype: CType)

sealed trait Segment {
  def id: SegmentId = SegmentId(blockid, cpath, ctype)

  def blockid: Long
  def cpath: CPath
  def ctype: CType
  def defined: BitSet
  def length: Int
  def extend(amount: Int): Segment
}

sealed trait ValueSegment[@spec(Boolean,Long,Double) A] extends Segment {
  def ctype: CValueType[A]
  def map[@spec(Boolean,Long,Double) B: CValueType: Manifest](f: A => B): ValueSegment[B]

  def normalize: ValueSegment[A] = this match {
    case seg: ArraySegment[_] if seg.ctype == CBoolean =>
      val values0 = seg.values.asInstanceOf[Array[Boolean]]
      val values = BitSetUtil.create()
      defined.foreach { row =>
        values(row) = values0(row)
      }
      BooleanSegment(blockid, cpath, defined, values, values.length).asInstanceOf[ValueSegment[A]]

    case _ =>
      this
  }
}

case class ArraySegment[@spec(Boolean,Long,Double) A](blockid: Long, cpath: CPath, ctype: CValueType[A], defined: BitSet, values: Array[A]) extends ValueSegment[A] {
  private implicit val m = ctype.manifest

  override def equals(that: Any): Boolean = that match {
    case ArraySegment(`blockid`, `cpath`, ct2, d2, values2) =>
      if (ctype != ct2 || defined != d2 || values2.length != values.length) return false
      var i = 0
      val len = values.length
      while (i < len) {
        if (values2(i) != values(i)) return false
        i += 1
      }
      true
    case _ =>
      false
  }

  def ++(rhs: ArraySegment[A]): ArraySegment[A] = rhs match {
    case ArraySegment(`blockid`, `cpath`, `ctype`, d2, values2) =>
      ArraySegment(blockid, cpath, ctype, defined ++ d2, (values ++ values2).toArray)
    case _ =>
      throw new IllegalArgumentException("mismatched Segments: %s and %s" format (this, rhs))
  }

  def length = values.length

  def extend(amount: Int) = {
    val arr = new Array[A](values.length + amount)
    var i = 0
    val len = values.length
    while (i < len) { arr(i) = values(i); i += 1 }
    ArraySegment(blockid, cpath, ctype, defined.copy, arr)
  }

  def map[@spec(Boolean,Long,Double) B: CValueType: Manifest](f: A => B): ValueSegment[B] = {
    val values0 = new Array[B](values.length)
    defined.foreach { row =>
      values0(row) = f(values(row))
    }
    ArraySegment[B](blockid, cpath, CValueType[B], defined, values0).normalize
  }
}

case class BooleanSegment(blockid: Long, cpath: CPath, defined: BitSet, values: BitSet, length: Int) extends ValueSegment[Boolean] {
  val ctype = CBoolean

  override def equals(that: Any) = that match {
    case BooleanSegment(`blockid`, `cpath`, d2, values2, `length`) => defined == d2 && values == values2
    case _ => false
  }

  def ++(rhs: BooleanSegment): BooleanSegment = rhs match {
    case BooleanSegment(`blockid`, `cpath`, d2, values2, length2) =>
      BooleanSegment(blockid, cpath, defined ++ d2, values ++ values2, length + length2)
    case _ =>
      throw new IllegalArgumentException("mismatched Segments: %s and %s" format (this, rhs))
  }

  def extend(amount: Int) = BooleanSegment(blockid, cpath, defined.copy, values.copy, length + amount)

  def map[@spec(Boolean,Long,Double) B: CValueType: Manifest](f: Boolean => B): ValueSegment[B] = {
    val values0 = new Array[B](values.length)
    defined.foreach { row =>
      values0(row) = f(values(row))
    }
    ArraySegment[B](blockid, cpath, CValueType[B], defined, values0).normalize
  }
}

case class NullSegment(blockid: Long, cpath: CPath, ctype: CNullType, defined: BitSet, length: Int) extends Segment {

  override def equals(that: Any) = that match {
    case NullSegment(`blockid`, `cpath`, `ctype`, d2, `length`) => defined == d2
    case _ => false
  }

  def ++(rhs: NullSegment): NullSegment = rhs match {
    case NullSegment(`blockid`, `cpath`, `ctype`, d2, length2) =>
      NullSegment(blockid, cpath, ctype, defined ++ d2, length + length2)
    case _ =>
      throw new IllegalArgumentException("mismatched Segments: %s and %s" format (this, rhs))
  }

  def extend(amount: Int) = NullSegment(blockid, cpath, ctype, defined.copy, length + amount)
}
