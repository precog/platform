package com.precog
package daze

import bytecode.Library

import yggdrasil._
import yggdrasil.table._

trait InfixLib extends ImplLibrary with GenOpcode {
  
  def PrimitiveEqualsF2 = yggdrasil.table.cf.std.Eq
  
  object Infix {
    val InfixNamespace = Vector("std", "infix")

    object Add extends Op2(InfixNamespace, "add") {  //TODO do we need cases for NullColumn?
      def f2: F2 = new CF2P({
        case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with LongColumn {
          def apply(row: Int) = c1(row) + c2(row)
        }
        case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with NumColumn {   //TODO why is c1(row) type BigDecimal?
          def apply(row: Int) = (c1(row): BigDecimal) + c2(row)
        }
        case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = c1(row) + c2(row)
        }
        case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = (c1(row): BigDecimal) + c2(row)
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
    }

    object Sub extends Op2(InfixNamespace, "subtract") {
      def f2: F2 = new CF2P({
        case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with LongColumn {
          def apply(row: Int) = c1(row) - c2(row)
        }
        case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = (c1(row): BigDecimal) - c2(row)
        }
        case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = c1(row) - c2(row)
        }
        case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = (c1(row): BigDecimal) - c2(row)
        }
        case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
          def apply(row: Int) = c1(row) - c2(row)
        }
        case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = c1(row) - c2(row)
        }
        case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = c1(row) - c2(row)
        }
        case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = c1(row) - c2(row)
        }
        case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = c1(row) - c2(row)
        }
      })
    }

    object Mul extends Op2(InfixNamespace, "multiply") {
      def f2: F2 = new CF2P({
        case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with LongColumn {
          def apply(row: Int) = c1(row) * c2(row)
        }
        case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = (c1(row): BigDecimal) * c2(row)
        }
        case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = c1(row) * c2(row)
        }
        case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = (c1(row): BigDecimal) * c2(row)
        }
        case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
          def apply(row: Int) = c1(row) * c2(row)
        }
        case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = c1(row) * c2(row)
        }
        case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = c1(row) * c2(row)
        }
        case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = c1(row) * c2(row)
        }
        case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = c1(row) * c2(row)
        }
      })
    }

    object Div extends Op2(InfixNamespace, "divide") {
      def f2: F2 = new CF2P({
        case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = (c1(row): BigDecimal) / c2(row)
        }
        case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = (c1(row): BigDecimal) / c2(row)
        }
        case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = c1(row) / c2(row)
        }
        case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = (c1(row): BigDecimal) / c2(row)
        }
        case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
          def apply(row: Int) = c1(row) / c2(row)
        }
        case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = c1(row) / c2(row)
        }
        case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = c1(row) / c2(row)
        }
        case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = c1(row) / c2(row)
        }
        case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with NumColumn {
          def apply(row: Int) = c1(row) / c2(row)
        }
      })
    }

    object Lt extends Op2(InfixNamespace, "lt") {
      def f2: F2 = new CF2P({
        case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) < c2(row)
        }
        case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) < c2(row)
        }
        case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) < c2(row)
        }
        case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) < c2(row)
        }
        case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) < c2(row)
        }
        case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) < c2(row)
        }
        case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) < c2(row)
        }
        case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) < c2(row)
        }
        case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) < c2(row)
        }
      })
    }

    object LtEq extends Op2(InfixNamespace, "lte") {
      def f2: F2 = new CF2P({
        case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) <= c2(row)
        }
        case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) <= c2(row)
        }
        case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) <= c2(row)
        }
        case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) <= c2(row)
        }
        case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) <= c2(row)
        }
        case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) <= c2(row)
        }
        case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) <= c2(row)
        }
        case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) <= c2(row)
        }
        case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) <= c2(row)
        }
      })
    }

    object Gt extends Op2(InfixNamespace, "gt") {
      def f2: F2 = new CF2P({
        case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) > c2(row)
        }
        case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) > c2(row)
        }
        case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) > c2(row)
        }
        case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) > c2(row)
        }
        case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) > c2(row)
        }
        case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) > c2(row)
        }
        case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) > c2(row)
        }
        case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) > c2(row)
        }
        case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) > c2(row)
        }
      })
    }

    object GtEq extends Op2(InfixNamespace, "gte") {
      def f2: F2 = new CF2P({
        case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) >= c2(row)
        }
        case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) >= c2(row)
        }
        case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) >= c2(row)
        }
        case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) >= c2(row)
        }
        case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) >= c2(row)
        }
        case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) >= c2(row)
        }
        case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) >= c2(row)
        }
        case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) >= c2(row)
        }
        case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) >= c2(row)
        }
      })
    }

    object And extends Op2(InfixNamespace, "and") {
      def f2: F2 = new CF2P({
        case (c1: BoolColumn, c2: BoolColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) && c2(row)
        }
      })
    }

    object Or extends Op2(InfixNamespace, "or") {
      def f2: F2 = new CF2P({
        case (c1: BoolColumn, c2: BoolColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) || c2(row)
        }
      })
    }
  }
}

