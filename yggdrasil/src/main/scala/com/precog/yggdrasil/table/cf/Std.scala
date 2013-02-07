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

import com.precog.util.NumericComparisons

object std {
  val Eq = CF2P("builtin::ct::eq") {
    case (c1: BoolColumn, c2: BoolColumn) => new Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = c1(row) == c2(row)
    }
    case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = c1(row) == c2(row)
    }
    case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = c1(row) == c2(row)
    }
    case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = BigDecimal(c1(row)) == c2(row)
    }
    case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = c1(row) == c2(row)
    }
    case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = c1(row) == c2(row)
    }
    case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = BigDecimal(c1(row)) == c2(row)
    }
    case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = c1(row) == BigDecimal(c2(row))
    }
    case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = c1(row) == BigDecimal(c2(row))
    }
    case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = c1(row) == c2(row)
    }
    case (c1: StrColumn, c2: StrColumn) => new Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = c1(row) == c2(row)
    }
    case (c1: DateColumn, c2: DateColumn) => new Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = {
        val res = NumericComparisons.compare(c1(row), c2(row))
        if (res == 0) true
        else false
      }
    }
    case (c1: EmptyObjectColumn, c2: EmptyObjectColumn) => new Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = true // always true where both columns are defined
    }
    case (c1: EmptyArrayColumn, c2: EmptyArrayColumn) => new Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = true // always true where both columns are defined
    }
    case (c1: NullColumn, c2: NullColumn) => new Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = true // always true where both columns are defined
    }
    case (c1, c2) => new Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = false   // equality is defined between all types
    }
  }

  val And = CF2P("builtin::ct::and") {
    case (c1: BoolColumn, c2: BoolColumn) => new Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = c1(row) && c2(row)
    }
  }
  
  val Or = CF2P("builtin::ct::or") {
    case (c1: BoolColumn, c2: BoolColumn) => new Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = c1(row) || c2(row)
    }
  }
}


// type Std
