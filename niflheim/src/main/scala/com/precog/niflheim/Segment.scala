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

sealed abstract class Segment(val blockid: Long, val cpath: CPath, val ctype: CType, val defined: BitSet) {
  def length: Int
  def extend(amount: Int): Segment
}

case class ArraySegment[A: ClassManifest](id: Long, cp: CPath, ct: CValueType[A], d: BitSet, val values: Array[A]) extends Segment(id, cp, ct, d) {

  def ++(rhs: ArraySegment[A]): ArraySegment[A] = rhs match {
    case ArraySegment(`id`, `cp`, `ct`, d2, values2) =>
      ArraySegment(id, cp, ct, d ++ d2, (values ++ values2).toArray)
    case _ =>
      throw new IllegalArgumentException("mismatched Segments: %s and %s" format (this, rhs))
  }

  def length = values.length

  def extend(amount: Int) = {
    val arr = new Array[A](values.length + amount)
    var i = 0
    val len = values.length
    while (i < len) { arr(i) = values(i); i += 1 }
    ArraySegment(id, cp, ct, d.copy, arr)
  }
}

case class BooleanSegment(id: Long, cp: CPath, d: BitSet, val values: BitSet, val length: Int) extends Segment(id, cp, CBoolean, d) {

  def ++(rhs: BooleanSegment): BooleanSegment = rhs match {
    case BooleanSegment(`id`, `cp`, d2, values2, length2) =>
      BooleanSegment(id, cp, d ++ d2, values ++ values2, length + length2)
    case _ =>
      throw new IllegalArgumentException("mismatched Segments: %s and %s" format (this, rhs))
  }

  def extend(amount: Int) = BooleanSegment(id, cp, d.copy, values.copy, length + amount)
}

case class NullSegment(id: Long, cp: CPath, ct: CNullType, d: BitSet, val length: Int) extends Segment(id, cp, ct, d) {

  def ++(rhs: NullSegment): NullSegment = rhs match {
    case NullSegment(`id`, `cp`, `ct`, d2, length2) =>
      NullSegment(id, cp, ct, d ++ d2, length + length2)
    case _ =>
      throw new IllegalArgumentException("mismatched Segments: %s and %s" format (this, rhs))
  }

  def extend(amount: Int) = NullSegment(id, cp, ct, d.copy, length + amount)
}
