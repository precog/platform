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

import org.apache.commons.collections.primitives.ArrayIntList

class CF1(f: Column => Option[Column]) extends (Column => Option[Column]) {
  def apply(c: Column): Option[Column] = f(c)

  // Do not use PartialFunction.compose or PartialFunction.andThen for composition,
  // because they will fail out with MatchError.
  def compose(f1: CF1): CF1 = new CF1(c => f1(c).flatMap(this))
  def andThen(f1: CF1): CF1 = new CF1(c => this(c).flatMap(f1))
}

class CF1P(f: PartialFunction[Column, Column]) extends CF1(f.lift)

class CF2(f: (Column, Column) => Option[Column]) extends ((Column, Column) => Option[Column]) {
  def apply(c1: Column, c2: Column): Option[Column] = f(c1, c2)
}

class CF2P(f: PartialFunction[(Column, Column), Column]) extends CF2(Function.untupled(f.lift))


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

case class Remap(f : PartialFunction[Int, Int]) extends CF1P ({
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
  def apply(indices: ArrayIntList) = new Remap({ case i if (i > 0 && i < indices.size) => indices.get(i) })
}
