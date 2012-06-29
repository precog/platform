package com.precog.yggdrasil
package table
package cf

object std {
  object Eq extends CF2P ({
    // TODO: MOAR CASES
    case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = c1(row) == c2(row)
    }
  })
}


// vim: set ts=4 sw=4 et:
