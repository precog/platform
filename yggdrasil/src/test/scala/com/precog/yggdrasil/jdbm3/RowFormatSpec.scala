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
package jdbm3

import com.precog.yggdrasil.table._
import com.precog.common.json._
import com.precog.util.ByteBufferPool

import org.joda.time.DateTime

import java.nio.ByteBuffer

import scala.collection.immutable.BitSet

import org.specs2._
import org.specs2.mutable.Specification
import org.scalacheck.{Shrink, Arbitrary, Gen}

class RowFormatSpec extends Specification with ScalaCheck with CValueGenerators {
  import Arbitrary._
  import ByteBufferPool._


  def genColumnRefs: Gen[List[ColumnRef]] = Gen.listOf(genCType) map (_.zipWithIndex map {
    case (cType, i) => ColumnRef(CPath(CPathIndex(i)), cType)
  })

  def genCValuesForColumnRefs(refs: List[ColumnRef]): Gen[List[CValue]] = Gen.sequence[List, CValue](refs map {
    case ColumnRef(_, cType) => Gen.frequency(5 -> genCValue(cType), 1 -> Gen.value(CUndefined))
  })

  def arrayColumnsFor(size: Int, refs: List[ColumnRef]): List[ArrayColumn[_]] =
    refs map JDBMSlice.columnFor(CPath.Identity, size) map (_._2)

  def verify(rows: List[List[CValue]], cols: List[Column]) = {
    rows.zipWithIndex foreach { case (values, row) =>
      (values zip cols) foreach (_ must beLike {
        case (CUndefined, col) if !col.isDefinedAt(row) => ok
        case (_, col) if !col.isDefinedAt(row) => ko
        case (CString(s), col: StrColumn) => col(row) must_== s
        case (CBoolean(x), col: BoolColumn) => col(row) must_== x
        case (CLong(x), col: LongColumn) => col(row) must_== x
        case (CDouble(x), col: DoubleColumn) => col(row) must_== x
        case (CNum(x), col: NumColumn) => col(row) must_== x
        case (CDate(x), col: DateColumn) => col(row) must_== x
        case (CNull, col: NullColumn) => ok
        case (CEmptyObject, col: EmptyObjectColumn) => ok
        case (CEmptyArray, col: EmptyArrayColumn) => ok
        case (CArray(xs, cType), col: HomogeneousArrayColumn[_]) if cType == col.tpe =>
          col(row) must_== xs
      })
    }
  }

  implicit lazy val arbColumnRefs = Arbitrary(genColumnRefs)

  implicit val shrinkCValues: Shrink[List[CValue]] = Shrink.shrinkAny[List[CValue]]
  implicit val shrinkRows: Shrink[List[List[CValue]]] = Shrink.shrinkAny[List[List[CValue]]]


  "RowFormat encoding/decoding" should {
    "survive round-trip from CValue -> Array[Byte] -> CValue" in {
      check { (refs: List[ColumnRef]) =>
        val rowFormat = RowFormat.forValues(refs)
        implicit val arbColumnValues: Arbitrary[List[CValue]] = Arbitrary(genCValuesForColumnRefs(refs))

        check { (vals: List[CValue]) =>
          assert(refs.size == vals.size)
          rowFormat.decode(rowFormat.encode(vals)) must_== vals
        }
      }
    }
    "survive rountrip from CValue -> Array[Byte] -> Column -> Array[Byte] -> CValue" in {
      val size = 10

      check { (refs: List[ColumnRef]) =>
        val rowFormat = RowFormat.forValues(refs)
        implicit val arbRows: Arbitrary[List[List[CValue]]] =
          Arbitrary(Gen.listOfN(size, genCValuesForColumnRefs(refs)))

        check { (rows: List[List[CValue]]) =>
          val columns = arrayColumnsFor(size, refs)
          val columnDecoder = rowFormat.ColumnDecoder(columns)
          val columnEncoder = rowFormat.ColumnEncoder(columns)

          // Fill up the columns with the values from the rows.
          rows.zipWithIndex foreach { case (vals, row) =>
            columnDecoder.decodeToRow(row, rowFormat.encode(vals))
          }

          verify(rows, columns)

          rows.zipWithIndex foreach { case (vals, row) =>
            rowFormat.decode(columnEncoder.encodeFromRow(row)) == vals
          }
        }
      }
    }
  }
}


