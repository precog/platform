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

import bytecode.{ BinaryOperationType, JNumberT, JBooleanT, Library }

import yggdrasil._
import yggdrasil.table._

trait InfixLib[M[+_]] extends GenOpcode[M] {
  
  def PrimitiveEqualsF2 = yggdrasil.table.cf.std.Eq
  
  object Infix {
    val InfixNamespace = Vector("std", "infix")

    object Add extends Op2(InfixNamespace, "add") {
      val tpe = BinaryOperationType(JNumberT, JNumberT, JBooleanT)
      def f2: F2 = new CF2P({
        case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with LongColumn {
          def apply(row: Int) = c1(row) + c2(row)
        }
        case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with NumColumn {
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
      val tpe = BinaryOperationType(JNumberT, JNumberT, JBooleanT)
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
      val tpe = BinaryOperationType(JNumberT, JNumberT, JBooleanT)
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
      val tpe = BinaryOperationType(JNumberT, JNumberT, JBooleanT)
      def f2: F2 = new CF2P({
        case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with NumColumn {
          override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
          def apply(row: Int) = (c1(row): BigDecimal) / c2(row)
        }
        case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with NumColumn {
          override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
          def apply(row: Int) = (c1(row): BigDecimal) / c2(row)
        }
        case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with NumColumn {
          override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
          def apply(row: Int) = c1(row) / c2(row)
        }
        case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with NumColumn {
          override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
          def apply(row: Int) = (c1(row): BigDecimal) / c2(row)
        }
        case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
          override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
          def apply(row: Int) = c1(row) / c2(row)
        }
        case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with NumColumn {
          override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
          def apply(row: Int) = c1(row) / c2(row)
        }
        case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with NumColumn {
          override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
          def apply(row: Int) = c1(row) / c2(row)
        }
        case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with NumColumn {
          override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
          def apply(row: Int) = c1(row) / c2(row)
        }
        case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with NumColumn {
          override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
          def apply(row: Int) = c1(row) / c2(row)
        }
      })
    }

    object Lt extends Op2(InfixNamespace, "lt") {
      val tpe = BinaryOperationType(JNumberT, JNumberT, JBooleanT)
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
      val tpe = BinaryOperationType(JNumberT, JNumberT, JBooleanT)
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
      val tpe = BinaryOperationType(JNumberT, JNumberT, JBooleanT)
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
      val tpe = BinaryOperationType(JNumberT, JNumberT, JBooleanT)
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
      val tpe = BinaryOperationType(JBooleanT, JBooleanT, JBooleanT)
      def f2: F2 = new CF2P({
        case (c1: BoolColumn, c2: BoolColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) && c2(row)
        }
      })
    }

    object Or extends Op2(InfixNamespace, "or") {
      val tpe = BinaryOperationType(JBooleanT, JBooleanT, JBooleanT)
      def f2: F2 = new CF2P({
        case (c1: BoolColumn, c2: BoolColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) || c2(row)
        }
      })
    }
  }
}

