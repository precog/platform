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

