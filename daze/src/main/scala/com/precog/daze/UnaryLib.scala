package com.precog
package daze

import bytecode.{ Library, UnaryOperationType, JNumberT, JBooleanT }

import yggdrasil._
import yggdrasil.table._

trait UnaryLib[M[+_]] extends GenOpcode[M] {
  def ConstantEmptyArray =
    new CF1(Function.const(Some(new InfiniteColumn with EmptyArrayColumn {})))
  
  object Unary {
    val UnaryNamespace = Vector("std", "unary")

    object Comp extends Op1(UnaryNamespace, "comp") {
      val tpe = UnaryOperationType(JBooleanT, JBooleanT)
      def f1: F1 = new CF1P({
        case c: BoolColumn => new Map1Column(c) with BoolColumn {
          def apply(row: Int) = !c(row)
        }
      })
    }
    
    object Neg extends Op1(UnaryNamespace, "neg") {
      val tpe = UnaryOperationType(JNumberT, JNumberT)
      def f1: F1 = new CF1P({
        case c: LongColumn => new Map1Column(c) with LongColumn {
          def apply(row: Int) = -c(row)
        }
        case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
          def apply(row: Int) = -c(row)
        }
        case c: NumColumn => new Map1Column(c) with NumColumn {
          def apply(row: Int) = -c(row)
        }
      })
    }
  }
}

