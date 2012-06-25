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

import com.precog.common._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary._
import scala.collection.BitSet

trait ArbitrarySlice extends ArbitraryProjectionDescriptor {
  def arbitraryBitSet(size: Int): Gen[BitSet] = {
    containerOfN[List, Boolean](size, arbitrary[Boolean]) map { BitsetColumn.bitset _ }
  }

  def genColumn(col: ColumnDescriptor, size: Int): Gen[Column] = {
    col.valueType match {
      case CStringArbitrary   => containerOfN[Array, String](size, arbitrary[String]) map ArrayStrColumn.apply _
      case CStringFixed(w)    => containerOfN[Array, String](size, arbitrary[String].filter(_.length < w)) map ArrayStrColumn.apply _
      case CBoolean           => containerOfN[Array, Boolean](size, arbitrary[Boolean]) map ArrayBoolColumn.apply _
      case CLong              => containerOfN[Array, Long](size, arbitrary[Long]) map ArrayLongColumn.apply _
      case CDouble            => containerOfN[Array, Double](size, arbitrary[Double]) map ArrayDoubleColumn.apply _
      case CDecimalArbitrary  => containerOfN[List, Double](size, arbitrary[Double]) map { arr => ArrayNumColumn(arr.map(v => BigDecimal(v)).toArray) }
      case CNull              => arbitraryBitSet(size) map { s => new BitsetColumn(s) with NullColumn }
      case CEmptyObject       => arbitraryBitSet(size) map { s => new BitsetColumn(s) with EmptyObjectColumn }
      case CEmptyArray        => arbitraryBitSet(size) map { s => new BitsetColumn(s) with EmptyArrayColumn }
    }
  }

  def genSlice(p: ProjectionDescriptor, sz: Int): Gen[Slice] = {
    def sequence[T](l: List[Gen[T]], acc: Gen[List[T]]): Gen[List[T]] = {
      l match {
        case x :: xs => acc.flatMap(l => sequence(xs, x.map(xv => xv :: l)))
        case Nil => acc
      }
    }

    for {
      ids <- listOfN(p.identities, listOfN(sz, arbitrary[Long]).map(_.sorted.toArray))
      data <- sequence(p.columns.map(cd => genColumn(cd, sz).map(col => (cd, col))), value(Nil))
    } yield {
      new Slice {
        val size = sz
        val columns: Map[ColumnRef, Column] = data.map({
          case (ColumnDescriptor(path, selector, ctype, _), arr) => (ColumnRef(selector, ctype) -> arr)
        })(collection.breakOut)
      }
    }
  }
}
