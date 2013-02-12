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

case class ArraySegment[A](blockid: Long, cpath: CPath, ctype: CValueType[A], defined: BitSet, values: Array[A]) extends Segment {
  private implicit val m = ctype.manifest

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
}

case class BooleanSegment(blockid: Long, cpath: CPath, defined: BitSet, values: BitSet, length: Int) extends Segment {
  val ctype = CBoolean

  def ++(rhs: BooleanSegment): BooleanSegment = rhs match {
    case BooleanSegment(`blockid`, `cpath`, d2, values2, length2) =>
      BooleanSegment(blockid, cpath, defined ++ d2, values ++ values2, length + length2)
    case _ =>
      throw new IllegalArgumentException("mismatched Segments: %s and %s" format (this, rhs))
  }

  def extend(amount: Int) = BooleanSegment(blockid, cpath, defined.copy, values.copy, length + amount)
}

case class NullSegment(blockid: Long, cpath: CPath, ctype: CNullType, defined: BitSet, length: Int) extends Segment {

  def ++(rhs: NullSegment): NullSegment = rhs match {
    case NullSegment(`blockid`, `cpath`, `ctype`, d2, length2) =>
      NullSegment(blockid, cpath, ctype, defined ++ d2, length + length2)
    case _ =>
      throw new IllegalArgumentException("mismatched Segments: %s and %s" format (this, rhs))
  }

  def extend(amount: Int) = NullSegment(blockid, cpath, ctype, defined.copy, length + amount)
}
