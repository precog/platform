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
package cf

import org.apache.commons.collections.primitives.ArrayIntList

import com.precog.common._
import com.precog.util.{BitSet, BitSetUtil, Loop}
import com.precog.util.BitSetUtil.Implicits._

import com.precog.util.DateTimeUtil._

object util {

  /**
   * Right-biased column union
   */
  val UnionRight = CF2P("builtin::ct::unionRight") {
    case (c1: BoolColumn, c2: BoolColumn) => new UnionColumn(c1, c2) with BoolColumn { 
      def apply(row: Int) = {
        if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else sys.error("Attempt to retrieve undefined value for row: " + row)
      } 
    }

    case (c1: LongColumn, c2: LongColumn) => new UnionColumn(c1, c2) with LongColumn { 
      def apply(row: Int) = {
        if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else sys.error("Attempt to retrieve undefined value for row: " + row)
      } 
    }

    case (c1: DoubleColumn, c2: DoubleColumn) => new UnionColumn(c1, c2) with DoubleColumn { 
      def apply(row: Int) = {
        if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else sys.error("Attempt to retrieve undefined value for row: " + row)
      } 
    }

    case (c1: NumColumn, c2: NumColumn) => new UnionColumn(c1, c2) with NumColumn { 
      def apply(row: Int) = {
        if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else sys.error("Attempt to retrieve undefined value for row: " + row)
      } 
    }

    case (c1: StrColumn, c2: StrColumn) => new UnionColumn(c1, c2) with StrColumn { 
      def apply(row: Int) = {
        if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else sys.error("Attempt to retrieve undefined value for row: " + row)
      } 
    }

    case (c1: DateColumn, c2: DateColumn) => new UnionColumn(c1, c2) with DateColumn { 
      def apply(row: Int) = {
        if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else sys.error("Attempt to retrieve undefined value for row: " + row)
      } 
    }

    case (c1: PeriodColumn, c2: PeriodColumn) => new UnionColumn(c1, c2) with PeriodColumn { 
      def apply(row: Int) = {
        if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else sys.error("Attempt to retrieve undefined value for row: " + row)
      } 
    }

    case (c1: HomogeneousArrayColumn[a], _c2: HomogeneousArrayColumn[_]) if c1.tpe == _c2.tpe =>
      val c2 = _c2.asInstanceOf[HomogeneousArrayColumn[a]]
      new UnionColumn(c1, c2) with HomogeneousArrayColumn[a] { 
        val tpe = c1.tpe
        def apply(row: Int) = {
          if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else sys.error("Attempt to retrieve undefined value for row: " + row)
        } 
      }


    case (c1: EmptyArrayColumn, c2: EmptyArrayColumn) => new UnionColumn(c1, c2) with EmptyArrayColumn
    case (c1: EmptyObjectColumn, c2: EmptyObjectColumn) => new UnionColumn(c1, c2) with EmptyObjectColumn
    case (c1: NullColumn, c2: NullColumn) => new UnionColumn(c1, c2) with NullColumn
  }

  case object NConcat {

    // Closest thing we can get to casting an array. This is completely unsafe.
    private def copyCastArray[A: Manifest](as: Array[_]): Array[A] = {
      var bs = new Array[A](as.length)
      System.arraycopy(as, 0, bs, 0, as.length)
      bs
    }

    def apply(cols: List[(Int, Column)]) = {
      val sortedCols = cols.sortBy(_._1)
      val offsets: Array[Int] = sortedCols.map(_._1)(collection.breakOut)
      val columns: Array[Column] = sortedCols.map(_._2)(collection.breakOut)

      cols match {
        case (_, _: BoolColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[BoolColumn]) => 
          val boolColumns = copyCastArray[BoolColumn](columns)
          Some(new NConcatColumn(offsets, boolColumns) with BoolColumn {
            def apply(row: Int) = {
              val i = indexOf(row)
              boolColumns(i)(row - offsets(i))
            }
          })

        case (_, _: LongColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[LongColumn]) =>
          val longColumns = copyCastArray[LongColumn](columns)
          Some(new NConcatColumn(offsets, longColumns) with LongColumn {
            def apply(row: Int) = {
              val i = indexOf(row)
              longColumns(i)(row - offsets(i))
            }
          })
        
        case (_, _: DoubleColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[DoubleColumn]) =>
          val doubleColumns = copyCastArray[DoubleColumn](columns)
          Some(new NConcatColumn(offsets, doubleColumns) with DoubleColumn {
            def apply(row: Int) = {
              val i = indexOf(row)
              doubleColumns(i)(row - offsets(i))
            }
          })

        case (_, _: NumColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[NumColumn]) =>
          val numColumns = copyCastArray[NumColumn](columns)
          Some(new NConcatColumn(offsets, numColumns) with NumColumn {
            def apply(row: Int) = {
              val i = indexOf(row)
              numColumns(i)(row - offsets(i))
            }
          })

        case (_, _: StrColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[StrColumn]) =>
          val strColumns = copyCastArray[StrColumn](columns)
          Some(new NConcatColumn(offsets, strColumns) with StrColumn {
            def apply(row: Int) = {
              val i = indexOf(row)
              strColumns(i)(row - offsets(i))
            }
          })

        case (_, _: DateColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[DateColumn]) =>
          val dateColumns = copyCastArray[DateColumn](columns)
          Some(new NConcatColumn(offsets, dateColumns) with DateColumn {
            def apply(row: Int) = {
              val i = indexOf(row)
              dateColumns(i)(row - offsets(i))
            }
          })

        case (_, _: PeriodColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[PeriodColumn]) =>
          val periodColumns = copyCastArray[PeriodColumn](columns)
          Some(new NConcatColumn(offsets, periodColumns) with PeriodColumn {
            def apply(row: Int) = {
              val i = indexOf(row)
              periodColumns(i)(row - offsets(i))
            }
          })

        case (_, _: EmptyArrayColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[EmptyArrayColumn]) =>
          val emptyArrayColumns = copyCastArray[EmptyArrayColumn](columns)
          Some(new NConcatColumn(offsets, emptyArrayColumns) with EmptyArrayColumn)

        case (_, _: EmptyObjectColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[EmptyObjectColumn]) =>
          val emptyObjectColumns = copyCastArray[EmptyObjectColumn](columns)
          Some(new NConcatColumn(offsets, emptyObjectColumns) with EmptyObjectColumn)

        case (_, _: NullColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[NullColumn]) =>
          val nullColumns = copyCastArray[NullColumn](columns)
          Some(new NConcatColumn(offsets, nullColumns) with NullColumn)

        case _ => None
      }
    }
  }

  //it would be nice to generalize these to `CoerceTo[A]`
  def CoerceToDouble = CF1P("builtin:ct:coerceToDouble") {
    case (c: DoubleColumn) => c

    case (c: LongColumn) => new Map1Column(c) with DoubleColumn {
      def apply(row: Int) = c(row).toDouble
    }

    case (c: NumColumn) => new Map1Column(c) with DoubleColumn {
      def apply(row: Int) = c(row).toDouble
    }
  }

  def CoerceToDate = CF1P("builtin:ct:coerceToDate") {
    case (c: DateColumn) => c

    case (c: StrColumn) => new DateColumn {
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && isValidISO(c(row))
      def apply(row: Int) = parseDateTime(c(row), true)
    }
  }

  def Concat(at: Int) = CF2P("builtin::ct::concat") {
    case (c1: BoolColumn, c2: BoolColumn) => new ConcatColumn(at, c1, c2) with BoolColumn { 
      def apply(row: Int) = if (row < at) c1(row) else c2(row - at)
    }

    case (c1: LongColumn, c2: LongColumn) => new ConcatColumn(at, c1, c2) with LongColumn { 
      def apply(row: Int) = if (row < at) c1(row) else c2(row - at)
    }

    case (c1: DoubleColumn, c2: DoubleColumn) => new ConcatColumn(at, c1, c2) with DoubleColumn { 
      def apply(row: Int) = if (row < at) c1(row) else c2(row - at)
    }

    case (c1: NumColumn, c2: NumColumn) => new ConcatColumn(at, c1, c2) with NumColumn { 
      def apply(row: Int) = if (row < at) c1(row) else c2(row - at)
    }

    case (c1: StrColumn, c2: StrColumn) => new ConcatColumn(at, c1, c2) with StrColumn { 
      def apply(row: Int) = if (row < at) c1(row) else c2(row - at)
    }

    case (c1: DateColumn, c2: DateColumn) => new ConcatColumn(at, c1, c2) with DateColumn { 
      def apply(row: Int) = if (row < at) c1(row) else c2(row - at)
    }

    case (c1: PeriodColumn, c2: PeriodColumn) => new ConcatColumn(at, c1, c2) with PeriodColumn { 
      def apply(row: Int) = if (row < at) c1(row) else c2(row - at)
    }

    case (c1: HomogeneousArrayColumn[a], _c2: HomogeneousArrayColumn[_]) if c1.tpe == _c2.tpe =>
      val c2 = _c2.asInstanceOf[HomogeneousArrayColumn[a]]
      new ConcatColumn(at, c1, c2) with HomogeneousArrayColumn[a] {
        val tpe = c1.tpe
        def apply(row: Int) = if (row < at) c1(row) else c2(row - at)
      }

    case (c1: EmptyArrayColumn, c2: EmptyArrayColumn) => new ConcatColumn(at, c1, c2) with EmptyArrayColumn
    case (c1: EmptyObjectColumn, c2: EmptyObjectColumn) => new ConcatColumn(at, c1, c2) with EmptyObjectColumn
    case (c1: NullColumn, c2: NullColumn) => new ConcatColumn(at, c1, c2) with NullColumn
  }

  def Shift(by: Int) = CF1P("builtin::ct::shift") {
    case c: BoolColumn => new ShiftColumn(by, c) with BoolColumn { 
      def apply(row: Int) = c(row - by)
    }

    case c: LongColumn => new ShiftColumn(by, c) with LongColumn { 
      def apply(row: Int) = c(row - by)
    }

    case c: DoubleColumn => new ShiftColumn(by, c) with DoubleColumn { 
      def apply(row: Int) = c(row - by)
    }

    case c: NumColumn => new ShiftColumn(by, c) with NumColumn { 
      def apply(row: Int) = c(row - by)
    }

    case c: StrColumn => new ShiftColumn(by, c) with StrColumn { 
      def apply(row: Int) = c(row - by)
    }

    case c: DateColumn => new ShiftColumn(by, c) with DateColumn { 
      def apply(row: Int) = c(row - by)
    }

    case c: PeriodColumn => new ShiftColumn(by, c) with PeriodColumn { 
      def apply(row: Int) = c(row - by)
    }

    case c: HomogeneousArrayColumn[a] => new ShiftColumn(by, c) with HomogeneousArrayColumn[a] { 
      val tpe = c.tpe
      def apply(row: Int) = c(row - by)
    }

    case c: EmptyArrayColumn => new ShiftColumn(by, c) with EmptyArrayColumn
    case c: EmptyObjectColumn => new ShiftColumn(by, c) with EmptyObjectColumn
    case c: NullColumn => new ShiftColumn(by, c) with NullColumn
  }

  def Sparsen(idx: Array[Int], toSize: Int) = CF1P("builtin::ct::sparsen") {
    case c: BoolColumn   => new SparsenColumn(c, idx, toSize) with BoolColumn { def apply(row: Int) = c(remap(row)) }
    case c: LongColumn   => new SparsenColumn(c, idx, toSize) with LongColumn { def apply(row: Int) = c(remap(row)) }
    case c: DoubleColumn => new SparsenColumn(c, idx, toSize) with DoubleColumn { def apply(row: Int) = c(remap(row)) }
    case c: NumColumn    => new SparsenColumn(c, idx, toSize) with NumColumn { def apply(row: Int) = c(remap(row)) }
    case c: StrColumn    => new SparsenColumn(c, idx, toSize) with StrColumn { def apply(row: Int) = c(remap(row)) }
    case c: DateColumn   => new SparsenColumn(c, idx, toSize) with DateColumn { def apply(row: Int) = c(remap(row)) }
    case c: PeriodColumn => new SparsenColumn(c, idx, toSize) with PeriodColumn { def apply(row: Int) = c(remap(row)) }
    case c: HomogeneousArrayColumn[a] => new SparsenColumn(c, idx, toSize) with HomogeneousArrayColumn[a] {
      val tpe = c.tpe
      def apply(row: Int) = c(remap(row))
    }

    case c: EmptyArrayColumn  => new SparsenColumn(c, idx, toSize) with EmptyArrayColumn
    case c: EmptyObjectColumn => new SparsenColumn(c, idx, toSize) with EmptyObjectColumn
    case c: NullColumn => new SparsenColumn(c, idx, toSize) with NullColumn
  }

  val Empty = CF1P("builtin::ct::empty") {
    case c: BoolColumn   => new EmptyColumn[BoolColumn] with BoolColumn
    case c: LongColumn   => new EmptyColumn[LongColumn] with LongColumn
    case c: DoubleColumn => new EmptyColumn[DoubleColumn] with DoubleColumn
    case c: NumColumn    => new EmptyColumn[NumColumn] with NumColumn
    case c: StrColumn    => new EmptyColumn[StrColumn] with StrColumn
    case c: DateColumn   => new EmptyColumn[DateColumn] with DateColumn
    case c: PeriodColumn => new EmptyColumn[PeriodColumn] with PeriodColumn
    case c: HomogeneousArrayColumn[a] => new EmptyColumn[HomogeneousArrayColumn[a]] with HomogeneousArrayColumn[a] {
      val tpe = c.tpe
    }
    case c: EmptyArrayColumn  => new EmptyColumn[EmptyArrayColumn] with EmptyArrayColumn
    case c: EmptyObjectColumn => new EmptyColumn[EmptyObjectColumn] with EmptyObjectColumn
    case c: NullColumn => new EmptyColumn[NullColumn] with NullColumn
  }

  def Remap(f: Int => Int) = CF1P ("builtin::ct::remap") {
    case c: BoolColumn   => new RemapColumn(c, f) with BoolColumn { def apply(row: Int) = c(f(row)) }
    case c: LongColumn   => new RemapColumn(c, f) with LongColumn { def apply(row: Int) = c(f(row)) }
    case c: DoubleColumn => new RemapColumn(c, f) with DoubleColumn { def apply(row: Int) = c(f(row)) }
    case c: NumColumn    => new RemapColumn(c, f) with NumColumn { def apply(row: Int) = c(f(row)) }
    case c: StrColumn    => new RemapColumn(c, f) with StrColumn { def apply(row: Int) = c(f(row)) }
    case c: DateColumn   => new RemapColumn(c, f) with DateColumn { def apply(row: Int) = c(f(row)) }
    case c: PeriodColumn => new RemapColumn(c, f) with PeriodColumn { def apply(row: Int) = c(f(row)) }
    case c: HomogeneousArrayColumn[a] => new RemapColumn(c, f) with HomogeneousArrayColumn[a] {
      val tpe = c.tpe
      def apply(row: Int) = c(f(row))
    }
    case c: EmptyArrayColumn  => new RemapColumn(c, f) with EmptyArrayColumn
    case c: EmptyObjectColumn => new RemapColumn(c, f) with EmptyObjectColumn
    case c: NullColumn => new RemapColumn(c, f) with NullColumn
  }

  def RemapFilter(filter: Int => Boolean, offset: Int) = CF1P("builtin::ct::remapFilter") {
    case c: BoolColumn   => new RemapFilterColumn(c, filter, offset) with BoolColumn { def apply(row: Int) = c(row + offset) }
    case c: LongColumn   => new RemapFilterColumn(c, filter, offset) with LongColumn { def apply(row: Int) = c(row + offset) }
    case c: DoubleColumn => new RemapFilterColumn(c, filter, offset) with DoubleColumn { def apply(row: Int) = c(row + offset) }
    case c: NumColumn    => new RemapFilterColumn(c, filter, offset) with NumColumn { def apply(row: Int) = c(row + offset) }
    case c: StrColumn    => new RemapFilterColumn(c, filter, offset) with StrColumn { def apply(row: Int) = c(row + offset) }
    case c: DateColumn   => new RemapFilterColumn(c, filter, offset) with DateColumn { def apply(row: Int) = c(row + offset) }
    case c: PeriodColumn => new RemapFilterColumn(c, filter, offset) with PeriodColumn { def apply(row: Int) = c(row + offset) }
    case c: HomogeneousArrayColumn[a] => new RemapFilterColumn(c, filter, offset) with HomogeneousArrayColumn[a] {
      val tpe = c.tpe
      def apply(row: Int) = c(row + offset)
    }
    case c: EmptyArrayColumn  => new RemapFilterColumn(c, filter, offset) with EmptyArrayColumn
    case c: EmptyObjectColumn => new RemapFilterColumn(c, filter, offset) with EmptyObjectColumn
    case c: NullColumn => new RemapFilterColumn(c, filter, offset) with NullColumn
  }

  def RemapIndices(indices: ArrayIntList) = CF1P("builtin::ct::remapIndices") {
    case c: BoolColumn   => new RemapIndicesColumn(c, indices) with BoolColumn { def apply(row: Int) = c(indices.get(row)) }
    case c: LongColumn   => new RemapIndicesColumn(c, indices) with LongColumn { def apply(row: Int) = c(indices.get(row)) }
    case c: DoubleColumn => new RemapIndicesColumn(c, indices) with DoubleColumn { def apply(row: Int) = c(indices.get(row)) }
    case c: NumColumn    => new RemapIndicesColumn(c, indices) with NumColumn { def apply(row: Int) = c(indices.get(row)) }
    case c: StrColumn    => new RemapIndicesColumn(c, indices) with StrColumn { def apply(row: Int) = c(indices.get(row)) }
    case c: DateColumn   => new RemapIndicesColumn(c, indices) with DateColumn { def apply(row: Int) = c(indices.get(row)) }
    case c: PeriodColumn => new RemapIndicesColumn(c, indices) with PeriodColumn { def apply(row: Int) = c(indices.get(row)) }
    case c: HomogeneousArrayColumn[a] => new RemapIndicesColumn(c, indices) with HomogeneousArrayColumn[a] {
      val tpe = c.tpe
      def apply(row: Int) = c(indices.get(row))
    }
    case c: EmptyArrayColumn  => new RemapIndicesColumn(c, indices) with EmptyArrayColumn
    case c: EmptyObjectColumn => new RemapIndicesColumn(c, indices) with EmptyObjectColumn
    case c: NullColumn => new RemapIndicesColumn(c, indices) with NullColumn
  }

  def filter(from: Int, to: Int, definedAt: BitSet) = CF1P("builtin::ct::filter") {
    case c: BoolColumn   => new BitsetColumn(definedAt & c.definedAt(from, to)) with BoolColumn { def apply(row: Int) = c(row) }
    case c: LongColumn   => new BitsetColumn(definedAt & c.definedAt(from, to)) with LongColumn { def apply(row: Int) = c(row) }
    case c: DoubleColumn => new BitsetColumn(definedAt & c.definedAt(from, to)) with DoubleColumn { def apply(row: Int) = c(row) }
    case c: NumColumn    => new BitsetColumn(definedAt & c.definedAt(from, to)) with NumColumn { def apply(row: Int) = c(row) }
    case c: StrColumn    => new BitsetColumn(definedAt & c.definedAt(from, to)) with StrColumn { def apply(row: Int) = c(row) }
    case c: DateColumn   => new BitsetColumn(definedAt & c.definedAt(from, to)) with DateColumn { def apply(row: Int) = c(row) }
    case c: PeriodColumn => new BitsetColumn(definedAt & c.definedAt(from, to)) with PeriodColumn { def apply(row: Int) = c(row) }
    case c: HomogeneousArrayColumn[a] =>
      new BitsetColumn(definedAt & c.definedAt(from, to)) with HomogeneousArrayColumn[a] {
        val tpe = c.tpe
        def apply(row: Int) = c(row)
      }
    case c: EmptyArrayColumn  => new BitsetColumn(definedAt & c.definedAt(from, to)) with EmptyArrayColumn
    case c: EmptyObjectColumn => new BitsetColumn(definedAt & c.definedAt(from, to)) with EmptyObjectColumn
    case c: NullColumn => new BitsetColumn(definedAt & c.definedAt(from, to)) with NullColumn
  }

  val isSatisfied = CF1P("builtin::ct::isSatisfied") {
    case c: BoolColumn => new BoolColumn {
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row)
      def apply(row: Int) = isDefinedAt(row)
    }
  }

  def FilterComplement(complement: Column) = CF1P("builtin::ct:filterComplement") {
    case c: BoolColumn   => new BoolColumn { 
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && !complement.isDefinedAt(row)
      def apply(row: Int) = c(row)
    }

    case c: LongColumn   => new LongColumn {
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && !complement.isDefinedAt(row)
      def apply(row: Int) = c(row)
    }
    case c: DoubleColumn => new DoubleColumn {
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && !complement.isDefinedAt(row)
      def apply(row: Int) = c(row)
    }
    case c: NumColumn    => new NumColumn {
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && !complement.isDefinedAt(row)
      def apply(row: Int) = c(row)
    }
    case c: StrColumn    => new StrColumn {
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && !complement.isDefinedAt(row)
      def apply(row: Int) = c(row)
    }
    case c: DateColumn   => new DateColumn {
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && !complement.isDefinedAt(row)
      def apply(row: Int) = c(row)
    }
    case c: PeriodColumn => new PeriodColumn {
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && !complement.isDefinedAt(row)
      def apply(row: Int) = c(row)
    }
    case c: HomogeneousArrayColumn[a] => new HomogeneousArrayColumn[a] {
      val tpe = c.tpe
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && !complement.isDefinedAt(row)
      def apply(row: Int) = c(row)
    }
    case c: EmptyArrayColumn  => new EmptyArrayColumn {
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && !complement.isDefinedAt(row)
    }
    case c: EmptyObjectColumn => new EmptyObjectColumn {
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && !complement.isDefinedAt(row)
    }
    case c: NullColumn => new NullColumn {
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && !complement.isDefinedAt(row)
    }
  }
  
  def DefinedConst(value: CValue) = CF1("builtin::ct::definedConst") { c => 
    Some(
      value match {
        case CString(s) => new StrColumn {
          def isDefinedAt(row: Int) = c.isDefinedAt(row)
          def apply(row: Int) = s
        }
        case CBoolean(b) => new BoolColumn {
          def isDefinedAt(row: Int) = c.isDefinedAt(row)
          def apply(row: Int) = b
        }
        case CLong(l) => new LongColumn {
          def isDefinedAt(row: Int) = c.isDefinedAt(row)
          def apply(row: Int) = l
        }
        case CDouble(d) => new DoubleColumn {
          def isDefinedAt(row: Int) = c.isDefinedAt(row)
          def apply(row: Int) = d
        }
        case CNum(n) => new NumColumn {
          def isDefinedAt(row: Int) = c.isDefinedAt(row)
          def apply(row: Int) = n
        }
        case CDate(d) => new DateColumn {
          def isDefinedAt(row: Int) = c.isDefinedAt(row)
          def apply(row: Int) = d
        }
        case CPeriod(p) => new PeriodColumn {
          def isDefinedAt(row: Int) = c.isDefinedAt(row)
          def apply(row: Int) = p
        }
        case value: CArray[a] => new HomogeneousArrayColumn[a] {
          val tpe = value.cType
          def isDefinedAt(row: Int) = c.isDefinedAt(row)
          def apply(row: Int) = value.value
        }
        case CNull => new NullColumn {
          def isDefinedAt(row: Int) = c.isDefinedAt(row)
        }
        case CEmptyObject => new EmptyObjectColumn {
          def isDefinedAt(row: Int) = c.isDefinedAt(row)
        }
        case CEmptyArray => new EmptyArrayColumn {
          def isDefinedAt(row: Int) = c.isDefinedAt(row)
        }
        case CUndefined => UndefinedColumn(c)
      }
    )
  }
  
  def MaskedUnion(leftMask: BitSet) = CF2P("builtin::ct::maskedUnion") {
    case (left: BoolColumn, right: BoolColumn) => new UnionColumn(left, right) with BoolColumn {
      def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
    }
  
    case (left: LongColumn, right: LongColumn) => new UnionColumn(left, right) with LongColumn {
      def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
    }
  
    case (left: DoubleColumn, right: DoubleColumn) => new UnionColumn(left, right) with DoubleColumn {
      def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
    }
  
    case (left: NumColumn, right: NumColumn) => new UnionColumn(left, right) with NumColumn {
      def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
    }
  
    case (left: StrColumn, right: StrColumn) => new UnionColumn(left, right) with StrColumn {
      def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
    }
  
    case (left: DateColumn, right: DateColumn) => new UnionColumn(left, right) with DateColumn {
      def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
    }
  
    case (left: PeriodColumn, right: PeriodColumn) => new UnionColumn(left, right) with PeriodColumn {
      def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
    }
  
    case (left: HomogeneousArrayColumn[a], right: HomogeneousArrayColumn[b]) if left.tpe == right.tpe => new UnionColumn(left, right) with HomogeneousArrayColumn[a] {
      val tpe = left.tpe
      def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row).asInstanceOf[Array[a]]
    }
  
    case (left: EmptyArrayColumn, right: EmptyArrayColumn) => new UnionColumn(left, right) with EmptyArrayColumn
    case (left: EmptyObjectColumn, right: EmptyObjectColumn) => new UnionColumn(left, right) with EmptyObjectColumn
    case (left: NullColumn, right: NullColumn) => new UnionColumn(left, right) with NullColumn
  }
}

// vim: set ts=4 sw=4 et:
