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
package functions

object CAdd extends CF2P ({
  case (c1: BoolColumn, c2: BoolColumn) => new BoolColumn {
    def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
    def apply(row: Int) = c1(row) || c2(row)
  }
  case (c1: LongColumn, c2: LongColumn) => new LongColumn {
    def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
    def apply(row: Int) = c1(row) + c2(row)
  }
  case (c1: DoubleColumn, c2: DoubleColumn) => new DoubleColumn {
    def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
    def apply(row: Int) = c1(row) + c2(row)
  }
  case (c1: NumColumn, c2: NumColumn) => new NumColumn {
    def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
    def apply(row: Int) = c1(row) + c2(row)
  }
})

// vim: set ts=4 sw=4 et:
