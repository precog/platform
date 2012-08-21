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

import scala.collection.BitSet
import org.apache.commons.collections.primitives.ArrayIntList

object util {

  /**
   * Right-biased column union
   */
  object UnionRight extends CF2P ({
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

    case (c1: EmptyArrayColumn, c2: EmptyArrayColumn) => new UnionColumn(c1, c2) with EmptyArrayColumn
    case (c1: EmptyObjectColumn, c2: EmptyObjectColumn) => new UnionColumn(c1, c2) with EmptyObjectColumn
    case (c1: NullColumn, c2: NullColumn) => new UnionColumn(c1, c2) with NullColumn
  })

  case class Concat(at: Int) extends CF2P({
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

    case (c1: EmptyArrayColumn, c2: EmptyArrayColumn) => new ConcatColumn(at, c1, c2) with EmptyArrayColumn
    case (c1: EmptyObjectColumn, c2: EmptyObjectColumn) => new ConcatColumn(at, c1, c2) with EmptyObjectColumn
    case (c1: NullColumn, c2: NullColumn) => new ConcatColumn(at, c1, c2) with NullColumn
  })

  case class Shift(by: Int) extends CF1P ({
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

    case c: EmptyArrayColumn => new ShiftColumn(by, c) with EmptyArrayColumn
    case c: EmptyObjectColumn => new ShiftColumn(by, c) with EmptyObjectColumn
    case c: NullColumn => new ShiftColumn(by, c) with NullColumn
  })

  case class Sparsen(idx: Array[Int], toSize: Int) extends CF1P ({
    case c: BoolColumn   => new SparsenColumn(c, idx, toSize) with BoolColumn { def apply(row: Int) = c(remap(row)) }
    case c: LongColumn   => new SparsenColumn(c, idx, toSize) with LongColumn { def apply(row: Int) = c(remap(row)) }
    case c: DoubleColumn => new SparsenColumn(c, idx, toSize) with DoubleColumn { def apply(row: Int) = c(remap(row)) }
    case c: NumColumn    => new SparsenColumn(c, idx, toSize) with NumColumn { def apply(row: Int) = c(remap(row)) }
    case c: StrColumn    => new SparsenColumn(c, idx, toSize) with StrColumn { def apply(row: Int) = c(remap(row)) }
    case c: DateColumn   => new SparsenColumn(c, idx, toSize) with DateColumn { def apply(row: Int) = c(remap(row)) }

    case c: EmptyArrayColumn  => new SparsenColumn(c, idx, toSize) with EmptyArrayColumn
    case c: EmptyObjectColumn => new SparsenColumn(c, idx, toSize) with EmptyObjectColumn
    case c: NullColumn => new SparsenColumn(c, idx, toSize) with NullColumn
  })

  case class Remap(f: PartialFunction[Int, Int]) extends CF1P ({
    case c: BoolColumn   => new RemapColumn(c, f) with BoolColumn { def apply(row: Int) = c(f(row)) }
    case c: LongColumn   => new RemapColumn(c, f) with LongColumn { def apply(row: Int) = c(f(row)) }
    case c: DoubleColumn => new RemapColumn(c, f) with DoubleColumn { def apply(row: Int) = c(f(row)) }
    case c: NumColumn    => new RemapColumn(c, f) with NumColumn { def apply(row: Int) = c(f(row)) }
    case c: StrColumn    => new RemapColumn(c, f) with StrColumn { def apply(row: Int) = c(f(row)) }
    case c: DateColumn   => new RemapColumn(c, f) with DateColumn { def apply(row: Int) = c(f(row)) }

    case c: EmptyArrayColumn  => new RemapColumn(c, f) with EmptyArrayColumn
    case c: EmptyObjectColumn => new RemapColumn(c, f) with EmptyObjectColumn
    case c: NullColumn => new RemapColumn(c, f) with NullColumn
  })

  object Remap {
    def forIndices(indices: ArrayIntList): Remap = Remap({ case i if (i >= 0 && i < indices.size) => indices.get(i) })
  }

  case class filter(from: Int, to: Int, definedAt: BitSet) extends CF1P ({
    case c: BoolColumn   => new BitsetColumn(definedAt & c.definedAt(from, to)) with BoolColumn { def apply(row: Int) = c(row) }
    case c: LongColumn   => new BitsetColumn(definedAt & c.definedAt(from, to)) with LongColumn { def apply(row: Int) = c(row) }
    case c: DoubleColumn => new BitsetColumn(definedAt & c.definedAt(from, to)) with DoubleColumn { def apply(row: Int) = c(row) }
    case c: NumColumn    => new BitsetColumn(definedAt & c.definedAt(from, to)) with NumColumn { def apply(row: Int) = c(row) }
    case c: StrColumn    => new BitsetColumn(definedAt & c.definedAt(from, to)) with StrColumn { def apply(row: Int) = c(row) }
    case c: DateColumn   => new BitsetColumn(definedAt & c.definedAt(from, to)) with DateColumn { def apply(row: Int) = c(row) }

    case c: EmptyArrayColumn  => new BitsetColumn(definedAt & c.definedAt(from, to)) with EmptyArrayColumn
    case c: EmptyObjectColumn => new BitsetColumn(definedAt & c.definedAt(from, to)) with EmptyObjectColumn
    case c: NullColumn => new BitsetColumn(definedAt & c.definedAt(from, to)) with NullColumn
  })

  object isSatisfied extends CF1P ({
    case c: BoolColumn => new BoolColumn {
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row)
      def apply(row: Int) = isDefinedAt(row)
    }
  })

  case class FilterComplement(complement: Column) extends CF1P({
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

    case c: EmptyArrayColumn  => new EmptyArrayColumn {
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && !complement.isDefinedAt(row)
    }
    case c: EmptyObjectColumn => new EmptyObjectColumn {
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && !complement.isDefinedAt(row)
    }
    case c: NullColumn => new NullColumn {
      def isDefinedAt(row: Int) = c.isDefinedAt(row) && !complement.isDefinedAt(row)
    }
  })
  
  case class DefinedConst(value: CValue) extends CF1(
    c => 
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
  )
}

// vim: set ts=4 sw=4 et:
