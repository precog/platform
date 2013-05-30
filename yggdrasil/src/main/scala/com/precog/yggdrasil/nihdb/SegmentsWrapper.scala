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
package nihdb

import org.slf4j.LoggerFactory

import com.precog.common._
import com.precog.util._
import com.precog.yggdrasil.table._
import com.precog.niflheim._

case class SegmentsWrapper(segments: Seq[Segment], projectionId: Int, blockId: Long) extends Slice {
  import TransSpecModule.paths

  val logger = LoggerFactory.getLogger("com.precog.yggdrasil.table.SegmentsWrapper")

  // FIXME: This should use an identity of Array[Long](projectionId,
  // blockId), but the evaluator will cry if we do that right now
  private def keyFor(row: Int): Long = {
    (projectionId.toLong << 44) ^ (blockId << 16) ^ row.toLong
  }

  private def buildKeyColumns(length: Int): Set[(ColumnRef, Column)] = {
    val hoId = (projectionId.toLong << 32) | (blockId >>> 32)
    val loId0 = (blockId & 0xFFFFFFFFL) << 32
    def loId(row: Int): Long = loId0 | row.toLong

    val hoKey = new LongColumn {
      def isDefinedAt(row: Int) = row >= 0 && row < length
      def apply(row: Int) = hoId
    }

    val loKey = new LongColumn {
      def isDefinedAt(row: Int) = row >= 0 && row < length
      def apply(row: Int) = loId(row)
    }

    Set((ColumnRef(CPath(paths.Key) \ 0 \ 0, CLong), loKey),
      (ColumnRef(CPath(paths.Key) \ 0 \ 1, CLong), hoKey))
  }

  private def buildKeyColumn(length: Int): (ColumnRef, Column) = {
    val keys = new Array[Long](length)
    var i = 0
    while (i < length) {
      keys(i) = keyFor(i)
      i += 1
    }
    (ColumnRef(CPath(paths.Key) \ 0, CLong), ArrayLongColumn(keys))
  }

  private def buildColumnRef(seg: Segment) = ColumnRef(CPath(paths.Value) \ seg.cpath, seg.ctype)

  private def buildColumn(seg: Segment): Column = seg match {
    case segment: ArraySegment[a] =>
      val ctype: CValueType[a] = segment.ctype
      val defined: BitSet = segment.defined
      val values: Array[a] = segment.values
      ctype match {
        case CString => new ArrayStrColumn(defined, values)
        case CDate => new ArrayDateColumn(defined, values)
        case CPeriod => new ArrayPeriodColumn(defined, values)
        case CNum => new ArrayNumColumn(defined, values)
        case CDouble => new ArrayDoubleColumn(defined, values)
        case CLong => new ArrayLongColumn(defined, values)
        case cat: CArrayType[_] => new ArrayHomogeneousArrayColumn(defined, values)(cat)
        case CBoolean => sys.error("impossible")
      }

    case BooleanSegment(_, _, defined, values, _) =>
      new ArrayBoolColumn(defined, values)

    case NullSegment(_, _, ctype, defined, _) => ctype match {
      case CNull =>
        NullColumn(defined)
      case CEmptyObject =>
        new MutableEmptyObjectColumn(defined)
      case CEmptyArray =>
        new MutableEmptyArrayColumn(defined)
      case CUndefined =>
        sys.error("also impossible")
    }
  }

  private def buildMap(segments: Seq[Segment]): Map[ColumnRef, Column] =
    segments.map(seg => (buildColumnRef(seg), buildColumn(seg))).toMap

  private val cols: Map[ColumnRef, Column] = buildMap(segments) + buildKeyColumn(segments.headOption map (_.length) getOrElse 0)

  val size: Int = {
    val sz = segments.foldLeft(0)(_ max _.length)
    if (logger.isTraceEnabled) {
      logger.trace("Computed size %d from:\n  %s".format(sz, segments.mkString("\n  ")))
    }
    sz
  }

  def columns: Map[ColumnRef, Column] = cols
}
