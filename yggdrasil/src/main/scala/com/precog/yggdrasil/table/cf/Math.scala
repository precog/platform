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

object math {
  object Negate extends CF1P({
    case c: BoolColumn => new Map1Column(c) with BoolColumn {
      def apply(row: Int) = !c(row)
    }
    case c: LongColumn => new Map1Column(c) with LongColumn {
      def apply(row: Int) = -c(row)
    }
    case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
      def apply(row: Int) = -c(row)
    }
    case c: NumColumn => new Map1Column(c) with NumColumn {
      def apply(row: Int) = -c(row)
    }
  })

  object Add extends CF2P({
    case (c1: BoolColumn, c2: BoolColumn) => new Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = c1(row) || c2(row)
    }
    case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with LongColumn {
      def apply(row: Int) = c1(row) + c2(row)
    }
    case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with NumColumn {
      def apply(row: Int) = c1(row) + c2(row)
    }
    case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with NumColumn {
      def apply(row: Int) = c1(row) + c2(row)
    }
    case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with NumColumn {
      def apply(row: Int) = c1(row) + c2(row)
    }
    case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
      def apply(row: Int) = c1(row) + c2(row)
    }
    case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with NumColumn {
      def apply(row: Int) = c1(row) + c2(row)
    }
    case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with NumColumn {
      def apply(row: Int) = c1(row) + c2(row)
    }
    case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with NumColumn {
      def apply(row: Int) = c1(row) + c2(row)
    }
    case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with NumColumn {
      def apply(row: Int) = c1(row) + c2(row)
    }
  })

  object Mod extends CF2P({
    case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with LongColumn {
      def apply(row: Int) = c1(row) % c2(row)
    }
    case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with NumColumn {
      def apply(row: Int) = c1(row) % c2(row)
    }
    case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with NumColumn {
      def apply(row: Int) = c1(row) % c2(row)
    }
    case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with NumColumn {
      def apply(row: Int) = c1(row) % c2(row)
    }
    case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
      def apply(row: Int) = c1(row) % c2(row)
    }
    case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with NumColumn {
      def apply(row: Int) = c1(row) % c2(row)
    }
    case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with NumColumn {
      def apply(row: Int) = c1(row) % c2(row)
    }
    case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with NumColumn {
      def apply(row: Int) = c1(row) % c2(row)
    }
    case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with NumColumn {
      def apply(row: Int) = c1(row) % c2(row)
    }
  })
}

// vim: set ts=4 sw=4 et:
