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
import com.precog.util.ByteBufferPool

import blueeyes.json.{ JPath, JPathIndex }

import org.joda.time.DateTime

import java.nio.ByteBuffer

import scala.collection.immutable.BitSet

import org.specs2._
import org.specs2.mutable.Specification
import org.scalacheck.{Shrink, Arbitrary, Gen}

import scala.annotation.tailrec

class RowFormatSpec extends Specification with ScalaCheck with CValueGenerators {
  import Arbitrary._
  import ByteBufferPool._


  // This should generate some jpath ids, then generate CTypes for these.
  // def genColumnRefs: Gen[List[ColumnRef]] = 
  def genColumnRefs: Gen[List[ColumnRef]] = Gen.listOf(Gen.alphaStr filter (_.size > 0)) flatMap { paths =>
    Gen.sequence[List, List[ColumnRef]](paths.distinct.map { name =>
      Gen.listOf(genCType) map { _.distinct map (ColumnRef(JPath(name), _)) }
    }).map(_.flatten)
  }

  // def genColumnRefs: Gen[List[ColumnRef]] = Gen.listOf(genCType) map (_.zipWithIndex map {
  //   case (cType, i) => ColumnRef(JPath(JPathIndex(i)), cType)
  // })

  def groupConsecutive[A, B](as: List[A])(f: A => B) = {
    @tailrec
    def build(as: List[A], prev: List[List[A]]): List[List[A]] = as match {
      case Nil =>
        prev.reverse
      case a :: _ =>
        val bs = as takeWhile { b => f(a) == f(b) }
        build(as drop bs.size, bs :: prev)
    }

    build(as, Nil)
  }

  def genCValuesForColumnRefs(refs: List[ColumnRef]): Gen[List[CValue]] = /*Gen.sequence[List, CValue](refs map {
    case ColumnRef(_, cType) => Gen.frequency(5 -> genCValue(cType), 1 -> Gen.value(CUndefined))
  })*/
  Gen.sequence[List, List[CValue]](groupConsecutive(refs)(_.selector) map {
    case refs =>
      Gen.choose(0, refs.size - 1) flatMap { i =>
        Gen.sequence[List, CValue](refs.zipWithIndex map {
          case (ColumnRef(_, cType), `i`) => Gen.frequency(5 -> genCValue(cType), 1 -> Gen.value(CUndefined))
          case (_, _) => Gen.value(CUndefined)
        })
      }
  }) map (_.flatten)

  def arrayColumnsFor(size: Int, refs: List[ColumnRef]): List[ArrayColumn[_]] =
    refs map JDBMSlice.columnFor(JPath.Identity, size) map (_._2)

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
      })
    }
  }

  implicit lazy val arbColumnRefs = Arbitrary(genColumnRefs)

  implicit val shrinkCValues: Shrink[List[CValue]] = Shrink.shrinkAny[List[CValue]]
  implicit val shrinkRows: Shrink[List[List[CValue]]] = Shrink.shrinkAny[List[List[CValue]]]

  "ValueRowFormat" should {
    checkRoundTrips(RowFormat.forValues(_))
  }

  "SortingKeyRowFormat" should {
    checkRoundTrips(RowFormat.forSortingKey(_))

    "sort encoded as ValueFormat does" in {
      check { refs: List[ColumnRef] =>
        val valueRowFormat = RowFormat.forValues(refs)
        val sortingKeyRowFormat = RowFormat.forSortingKey(refs)
        implicit val arbRows: Arbitrary[List[List[CValue]]] =
          Arbitrary(Gen.listOfN(10, genCValuesForColumnRefs(refs)))



        check { (vals: List[List[CValue]]) =>
          val valueEncoded = vals map (valueRowFormat.encode(_))
          val sortEncoded = vals map (sortingKeyRowFormat.encode(_))

          val sortedA = valueEncoded.sorted(new Ordering[Array[Byte]] {
            def compare(a: Array[Byte], b: Array[Byte]) = valueRowFormat.compare(a, b)
          }) map (valueRowFormat.decode(_))
          val sortedB = sortEncoded.sorted(new Ordering[Array[Byte]] {
            def compare(a: Array[Byte], b: Array[Byte]) = sortingKeyRowFormat.compare(a, b)
          }) map (sortingKeyRowFormat.decode(_))

          sortedA must_== sortedB
        }
      }
    }.set(minTestsOk -> 2000).pendingUntilFixed  //IMPORTANT: unless you just fixed this test, it is probably still broken
  }

  def checkRoundTrips(toRowFormat: List[ColumnRef] => RowFormat) {
    "survive round-trip from CValue -> Array[Byte] -> CValue" in {
      check { (refs: List[ColumnRef]) =>
        val rowFormat = toRowFormat(refs)
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
        val rowFormat = toRowFormat(refs)
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


