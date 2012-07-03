package com.precog.yggdrasil
package table
package cf

object std {
  object Eq extends CF2P ({
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
      def apply(row: Int) = c1(row) == c2(row)
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
  })

  object And extends CF2P ({
    case (c1: BoolColumn, c2: BoolColumn) => new Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = c1(row) && c2(row)
    }
  })

  object Or extends CF2P ({
    case (c1: BoolColumn, c2: BoolColumn) => new Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = c1(row) || c2(row)
    }
  })
}


// vim: set ts=4 sw=4 et:
