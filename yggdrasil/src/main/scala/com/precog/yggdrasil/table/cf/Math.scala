package com.precog.yggdrasil
package table
package cf

object math {
  val Negate = CF1P("builtin::ct::negate") {
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
  }

  val Add = CF2P("builtin::ct::add") {
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
  }

  val Mod = CF2P("builtin::ct::mod") {
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
  }
}

// type Math
