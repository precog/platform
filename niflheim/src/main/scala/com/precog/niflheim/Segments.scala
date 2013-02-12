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

import blueeyes.json._
import scala.collection.mutable
import org.joda.time.DateTime
import java.io._

import com.precog.common._
import com.precog.common.json._
import com.precog.util._

object Segments {
  def empty(id: Long): Segments =
    new Segments(id, 0, mutable.Map.empty[(CPath, CType), Int], mutable.ArrayBuffer.empty[Segment])
}

class Segments(id: Long, var length: Int, m: mutable.Map[(CPath, CType), Int], a: mutable.ArrayBuffer[Segment]) {
  def copy: Segments = new Segments(id, length, m.clone, a.clone)

  def addNullType(row: Int, cpath: CPath, ct: CNullType) {
    val k = (cpath, ct)
    if (m.contains(k)) {
      a(m(k)).defined.set(row)
    } else {
      m(k) = a.length
      val d = new BitSet()
      d.set(row)
      a.append(NullSegment(id, cpath, ct, d, length))
    }
  }

  def addNull(row: Int, cpath: CPath): Unit = addNullType(row, cpath, CNull)

  def addEmptyArray(row: Int, cpath: CPath): Unit = addNullType(row, cpath, CEmptyArray)

  def addEmptyObject(row: Int, cpath: CPath): Unit = addNullType(row, cpath, CEmptyObject)

  def addTrue(row: Int, cpath: CPath) {
    val k = (cpath, CBoolean)
    if (m.contains(k)) {
      val seg = a(m(k)).asInstanceOf[BooleanSegment]
      seg.defined.set(row)
      seg.values.set(row)
    } else {
      m(k) = a.length
      val d = new BitSet()
      val v = new BitSet()
      d.set(row)
      v.set(row)
      a.append(BooleanSegment(id, cpath, d, v, length))
    }
  }

  def addFalse(row: Int, cpath: CPath) {
    val k = (cpath, CBoolean)
    if (m.contains(k)) {
      a(m(k)).defined.set(row)
    } else {
      m(k) = a.length
      val d = new BitSet()
      val v = new BitSet()
      d.set(row)
      a.append(BooleanSegment(id, cpath, d, v, length))
    }
  }

  def addString(row: Int, cpath: CPath, s: String) {
    val k = (cpath, CBoolean)
    if (m.contains(k)) {
      val seg = a(m(k)).asInstanceOf[ArraySegment[String]]
      seg.defined.set(row)
      seg.values(row) = s
    } else {
      m(k) = a.length
      val d = new BitSet()
      d.set(row)
      val v = new Array[String](length)
      v(row) = s
      a.append(ArraySegment[String](id, cpath, CString, d, v))
    }
  }

  def addLong(row: Int, cpath: CPath, n: Long) {
    val k = (cpath, CLong)
    if (m.contains(k)) {
      val seg = a(m(k)).asInstanceOf[ArraySegment[Long]]
      seg.defined.set(row)
      seg.values(row) = n
    } else {
      m(k) = a.length
      val d = new BitSet()
      d.set(row)
      val v = new Array[Long](length)
      v(row) = n
      a.append(ArraySegment[Long](id, cpath, CLong, d, v))
    }
  }

  def addDouble(row: Int, cpath: CPath, n: Double) {
    val k = (cpath, CDouble)
    if (m.contains(k)) {
      val seg = a(m(k)).asInstanceOf[ArraySegment[Double]]
      seg.defined.set(row)
      seg.values(row) = n
    } else {
      m(k) = a.length
      val d = new BitSet()
      d.set(row)
      val v = new Array[Double](length)
      v(row) = n
      a.append(ArraySegment[Double](id, cpath, CDouble, d, v))
    }
  }

  def addBigDecimal(row: Int, cpath: CPath, n: BigDecimal) {
    val k = (cpath, CNum)
    if (m.contains(k)) {
      val seg = a(m(k)).asInstanceOf[ArraySegment[BigDecimal]]
      seg.defined.set(row)
      seg.values(row) = n
    } else {
      m(k) = a.length
      val d = new BitSet()
      d.set(row)
      val v = new Array[BigDecimal](length)
      v(row) = n
      a.append(ArraySegment[BigDecimal](id, cpath, CNum, d, v))
    }
  }

  def addNum(row: Int, cpath: CPath, s: String): Unit =
    addBigDecimal(row, cpath, BigDecimal(s)) //FIXME?

  def extendWithRows(rows: Seq[JValue]) {
    var i = 0

    val rlen = rows.length

    val alen = a.length
    while (i < alen) {
      a(i) = a(i).extend(rlen)
      i += 1
    }

    i = length
    length += rlen

    rows.foreach { j =>
      initializeSegments(i, j, Nil)
      i += 1
    }
  }

  def initializeSegments(row: Int, j: JValue, nodes: List[CPathNode]): Unit = j match {
    case JNull => addNull(row, CPath(nodes.reverse))
    case JTrue => addTrue(row, CPath(nodes.reverse))
    case JFalse => addFalse(row, CPath(nodes.reverse))

    case JString(s) => addString(row, CPath(nodes.reverse), s)
    case JNumLong(n) => addLong(row, CPath(nodes.reverse), n)
    case JNumDouble(n) => addDouble(row, CPath(nodes.reverse), n)
    case JNumBigDec(n) => addBigDecimal(row, CPath(nodes.reverse), n)
    case JNumStr(s) => addNum(row, CPath(nodes.reverse), s)

    case JObject(m) =>
      if (m.isEmpty) {
        addEmptyObject(row, CPath(nodes.reverse))
      } else {
        m.foreach {
          case (key, j) => initializeSegments(row, j, CPathField(key) :: nodes)
        }
      }

    case JArray(js) =>
      if (js.isEmpty) {
        addEmptyArray(row, CPath(nodes.reverse))
      } else {
        var i = 0
        js.foreach { j =>
          initializeSegments(row, j, CPathIndex(i) :: nodes)
          i += 1
        }
      }

    case JUndefined => ()
  }
}
