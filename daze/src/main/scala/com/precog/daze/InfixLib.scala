package com.precog
package daze

import bytecode.{ BinaryOperationType, JNumberT, JBooleanT, JTextT, Library }

import yggdrasil._
import yggdrasil.table._

trait InfixLib[M[+_]] extends GenOpcode[M] {
  
  def PrimitiveEqualsF2 = yggdrasil.table.cf.std.Eq
  
  object Infix {
    val InfixNamespace = Vector("std", "infix")

    val Add = new Op2(InfixNamespace, "add") {
      val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)
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

    val Sub = new Op2(InfixNamespace, "subtract") {
      val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)
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

    val Mul = new Op2(InfixNamespace, "multiply") {
      val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)
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

    val Div = new Op2(InfixNamespace, "divide") {
      val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)
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

    val Mod = new Op2(InfixNamespace, "mod") {
      val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)
      def f2: F2 = new CF2P({
        case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with LongColumn {
          override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
          def apply(row: Int) = {
            val c1r = c1(row) 
            val c2r = c2(row)

            if ((c1r ^ c2r) < 0)
              (c1r % c2r) + c2r
            else
              c1r % c2r
          }
        }
        case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with NumColumn {
          override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
          def apply(row: Int) = {
            val c1r = c1(row): BigDecimal 
            val c2r = c2(row): BigDecimal

            if ((c1r < 0 && c2r >= 0) || (c1r >= 0 && c2r < 0))
              (c1r % c2r) + c2r
            else
              c1r % c2r
          }
        }
        case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with NumColumn {
          override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
          def apply(row: Int) = {
            val c1r = c1(row): BigDecimal 
            val c2r = c2(row)

            if ((c1r < 0 && c2r >= 0) || (c1r >= 0 && c2r < 0))
              (c1r % c2r) + c2r
            else
              c1r % c2r
          }
        }
        case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
          override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
          def apply(row: Int) = {
            val c1r = c1(row) 
            val c2r = c2(row)

            if ((c1r < 0 && c2r >= 0) || (c1r >= 0 && c2r < 0))
              (c1r % c2r) + c2r
            else
              c1r % c2r
          }
        }
        case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with NumColumn {
          override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
          def apply(row: Int) = {
            val c1r = c1(row): BigDecimal 
            val c2r = c2(row): BigDecimal

            if ((c1r < 0 && c2r >= 0) || (c1r >= 0 && c2r < 0))
              (c1r % c2r) + c2r
            else
              c1r % c2r
          }
        }
        case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with NumColumn {
          override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
          def apply(row: Int) = {
            val c1r = c1(row): BigDecimal 
            val c2r = c2(row)

            if ((c1r < 0 && c2r >= 0) || (c1r >= 0 && c2r < 0))
              (c1r % c2r) + c2r
            else
              c1r % c2r
          }
        }
        case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with NumColumn {
          override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
          def apply(row: Int) = {
            val c1r = c1(row) 
            val c2r = c2(row)

            if ((c1r < 0 && c2r >= 0) || (c1r >= 0 && c2r < 0))
              (c1r % c2r) + c2r
            else
              c1r % c2r
          }
        }
        case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with NumColumn {
          override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
          def apply(row: Int) = {
            val c1r = c1(row) 
            val c2r = c2(row): BigDecimal

            if ((c1r < 0 && c2r >= 0) || (c1r >= 0 && c2r < 0))
              (c1r % c2r) + c2r
            else
              c1r % c2r
          }
        }
        case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with NumColumn {
          override def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
          def apply(row: Int) = {
            val c1r = c1(row) 
            val c2r = c2(row): BigDecimal

            if ((c1r < 0 && c2r >= 0) || (c1r >= 0 && c2r < 0))
              (c1r % c2r) + c2r
            else
              c1r % c2r
          }
        }
      })
    }

    val Lt = new Op2(InfixNamespace, "lt") {
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

    val LtEq = new Op2(InfixNamespace, "lte") {
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

    val Gt = new Op2(InfixNamespace, "gt") {
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

    val GtEq = new Op2(InfixNamespace, "gte") {
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

    val And = new Op2(InfixNamespace, "and") {
      val tpe = BinaryOperationType(JBooleanT, JBooleanT, JBooleanT)
      def f2: F2 = new CF2P({
        case (c1: BoolColumn, c2: BoolColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) && c2(row)
        }
      })
    }

    val Or = new Op2(InfixNamespace, "or") {
      val tpe = BinaryOperationType(JBooleanT, JBooleanT, JBooleanT)
      def f2: F2 = new CF2P({
        case (c1: BoolColumn, c2: BoolColumn) => new Map2Column(c1, c2) with BoolColumn {
          def apply(row: Int) = c1(row) || c2(row)
        }
      })
    }
    
    val concatString = new Op2(InfixNamespace, "concatString") {
      val tpe = BinaryOperationType(JTextT, JTextT, JTextT)
      def f2: F2 = new CF2P({
        case (c1: StrColumn, c2: StrColumn) => new Map2Column(c1, c2) with StrColumn {
          def apply(row: Int) = c1(row) + c2(row)
        }
      })
    }

  }
}

