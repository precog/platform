package com.precog
package daze

import bytecode.Library
import bytecode.BuiltInFunc1
import bytecode.BuiltInFunc2
import yggdrasil._

object InfixLib extends InfixLib

trait InfixLib extends ImplLibrary with GenOpcode {
  type F1 = CF1P
  type F2 = CF2P
  
  def PrimitiveEqualsF2 = new CF2P({
    case (c1: LongColumn, c2: LongColumn) => new BoolColumn {
      def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
      def apply(row: Int) = c1(row) == c2(row)
    }
    case (c1: DoubleColumn, c2: DoubleColumn) => new BoolColumn {
      def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
      def apply(row: Int) = c1(row) == c2(row)
    }
    case (c1: NumColumn, c2: NumColumn) => new BoolColumn {
      def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
      def apply(row: Int) = c1(row) == c2(row)
    }
    case (c1: StringColumn, c2: StringColumn) => new BoolColumn {
      def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
      def apply(row: Int) = c1(row) == c2(row)
    }
    case (c1: BoolColumn, c2: BoolColumn) => new BoolColumn {
      def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
      def apply(row: Int) = !(c1(row) ^ c2(row))      // when equals just isn't fast enough!
    }
    case (c1: NullColumn, c2: NullColumn) => new BoolColumn {
      def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
      def apply(row: Int) = true
    }
    case (c1: EmptyObjectColumn, c2: EmptyObjectColumn) => new BoolColumn {
      def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
      def apply(row: Int) = true
    }
    case (c1: EmptyArrayColumn, c2: EmptyArrayColumn) => new BoolColumn {
      def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
      def apply(row: Int) = true
    }
  })
  
  object Infix {
    val InfixNamespace = Vector("std", "infix")

    object Add extends Op2(InfixNamespace, "add") {
      def f2: F2 = new CF2P({
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
    }

    object Sub extends Op2(InfixNamespace, "subtract") {
      def f2: F2 = new CF2P({
        case (c1: LongColumn, c2: LongColumn) => new LongColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
          def apply(row: Int) = c1(row) - c2(row)
        }
        case (c1: DoubleColumn, c2: DoubleColumn) => new DoubleColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
          def apply(row: Int) = c1(row) - c2(row)
        }
        case (c1: NumColumn, c2: NumColumn) => new NumColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
          def apply(row: Int) = c1(row) - c2(row)
        }
      })
    }

    object Mul extends Op2(InfixNamespace, "multiply") {
      def f2: F2 = new CF2P({
        case (c1: LongColumn, c2: LongColumn) => new LongColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
          def apply(row: Int) = c1(row) * c2(row)
        }
        case (c1: DoubleColumn, c2: DoubleColumn) => new DoubleColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
          def apply(row: Int) = c1(row) * c2(row)
        }
        case (c1: NumColumn, c2: NumColumn) => new NumColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
          def apply(row: Int) = c1(row) * c2(row)
        }
      })
    }

    object Div extends Op2(InfixNamespace, "divide") {
      def f2: F2 = new CF2P({
        case (c1: LongColumn, c2: LongColumn) => new LongColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
          def apply(row: Int) = c1(row) / c2(row)
        }
        case (c1: DoubleColumn, c2: DoubleColumn) => new DoubleColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
          def apply(row: Int) = c1(row) / c2(row)
        }
        case (c1: NumColumn, c2: NumColumn) => new NumColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
          def apply(row: Int) = c1(row) / c2(row)
        }
      })
    }

    object Lt extends Op2(InfixNamespace, "lt") {
      def f2: F2 = new CF2P({
        case (c1: LongColumn, c2: LongColumn) => new BoolColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
          def apply(row: Int) = c1(row) < c2(row)
        }
        case (c1: DoubleColumn, c2: DoubleColumn) => new BoolColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
          def apply(row: Int) = c1(row) < c2(row)
        }
        case (c1: NumColumn, c2: NumColumn) => new BoolColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
          def apply(row: Int) = c1(row) < c2(row)
        }
      })
    }

    object LtEq extends Op2(InfixNamespace, "lte") {
      def f2: F2 = new CF2P({
        case (c1: LongColumn, c2: LongColumn) => new BoolColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
          def apply(row: Int) = c1(row) <= c2(row)
        }
        case (c1: DoubleColumn, c2: DoubleColumn) => new BoolColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
          def apply(row: Int) = c1(row) <= c2(row)
        }
        case (c1: NumColumn, c2: NumColumn) => new BoolColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
          def apply(row: Int) = c1(row) <= c2(row)
        }
      })
    }

    object Gt extends Op2(InfixNamespace, "gt") {
      def f2: F2 = new CF2P({
        case (c1: LongColumn, c2: LongColumn) => new BoolColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
          def apply(row: Int) = c1(row) > c2(row)
        }
        case (c1: DoubleColumn, c2: DoubleColumn) => new BoolColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
          def apply(row: Int) = c1(row) > c2(row)
        }
        case (c1: NumColumn, c2: NumColumn) => new BoolColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
          def apply(row: Int) = c1(row) > c2(row)
        }
      })
    }

    object GtEq extends Op2(InfixNamespace, "gte") {
      def f2: F2 = new CF2P({
        case (c1: LongColumn, c2: LongColumn) => new BoolColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
          def apply(row: Int) = c1(row) >= c2(row)
        }
        case (c1: DoubleColumn, c2: DoubleColumn) => new BoolColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
          def apply(row: Int) = c1(row) >= c2(row)
        }
        case (c1: NumColumn, c2: NumColumn) => new BoolColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
          def apply(row: Int) = c1(row) >= c2(row)
        }
      })
    }

    object And extends Op2(InfixNamespace, "and") {
      def f2: F2 = new CF2P({
        case (c1: BoolColumn, c2: BoolColumn) => new BoolColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
          def apply(row: Int) = c1(row) && c2(row)
        }
      })
    }

    object Or extends Op2(InfixNamespace, "or") {
      def f2: F2 = new CF2P({
        case (c1: BoolColumn, c2: BoolColumn) => new BoolColumn {
          def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row)
          def apply(row: Int) = c1(row) || c2(row)
        }
      })
    }
  }
}

