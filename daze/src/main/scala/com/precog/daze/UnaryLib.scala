package com.precog
package daze

import bytecode.{ Library, UnaryOperationType, JNumberT, JBooleanT }

import yggdrasil._
import yggdrasil.table._

trait UnaryLib[M[+_]] extends GenOpcode[M] {

  import StdLib.{BoolFrom, DoubleFrom, LongFrom, NumFrom, StrFrom, doubleIsDefined}

  def ConstantEmptyArray =
    CF1("builtin::const::emptyArray") { Function.const(Some(new InfiniteColumn with EmptyArrayColumn {})) }
  
  object Unary {
    val UnaryNamespace = Vector("std", "unary")

    object Comp extends Op1(UnaryNamespace, "comp") {
      val tpe = UnaryOperationType(JBooleanT, JBooleanT)
      def f1(ctx: EvaluationContext): F1 = CF1P("builtin::unary::comp") {
        case c: BoolColumn => new BoolFrom.B(c, !_)
      }
    }
    
    object Neg extends Op1(UnaryNamespace, "neg") {
      val tpe = UnaryOperationType(JNumberT, JNumberT)
      def f1(ctx: EvaluationContext): F1 = CF1P("builtin::unary::neg") {
        case c: DoubleColumn => new DoubleFrom.D(c, doubleIsDefined, -_)
        case c: LongColumn => new LongFrom.L(c, n => true, -_)
        case c: NumColumn => new NumFrom.N(c, n => true, -_)
      }
    }
  }
}
