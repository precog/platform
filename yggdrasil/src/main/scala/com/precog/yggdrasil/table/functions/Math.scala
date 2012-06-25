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
