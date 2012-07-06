package com.precog
package daze

import bytecode.Library

import yggdrasil._
import yggdrasil.table._

object UnaryLib extends UnaryLib

trait UnaryLib extends ImplLibrary with GenOpcode {
  object Unary {
    val UnaryNamespace = Vector("std", "unary")

    object Comp extends Op1(UnaryNamespace, "comp") {
      def f1: F1 = new CF1P({
        case c: BoolColumn => new Map1Column(c) with BoolColumn {
          def apply(row: Int) = !c(row)
        }
      })
    }
    
    object Neg extends Op1(UnaryNamespace, "neg") {
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

