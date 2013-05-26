package com.precog
package daze

import bytecode.{ Library, UnaryOperationType, JNumberT, JBooleanT }

import yggdrasil._
import yggdrasil.table._

import TransSpecModule._

trait UnaryLibModule[M[+_]] extends ColumnarTableLibModule[M] {
  trait UnaryLib extends ColumnarTableLib {
    import trans._
    import StdLib.{BoolFrom, DoubleFrom, LongFrom, NumFrom, StrFrom, doubleIsDefined}
    
    def ConstantEmptyArray =
      CF1("builtin::const::emptyArray") { Function.const(Some(new InfiniteColumn with EmptyArrayColumn {})) }
    
    object Unary {
      val UnaryNamespace = Vector("std", "unary")
  
      object Comp extends Op1F1(UnaryNamespace, "comp") {
        val tpe = UnaryOperationType(JBooleanT, JBooleanT)
        def f1(ctx: MorphContext): F1 = CF1P("builtin::unary::comp") {
          case c: BoolColumn => new BoolFrom.B(c, !_)
        }

        def spec[A <: SourceType](ctx: MorphContext): TransSpec[A] => TransSpec[A] = {
          transSpec => trans.Map1(transSpec, f1(ctx))
        }
      }
      
      object Neg extends Op1F1(UnaryNamespace, "neg") {
        val tpe = UnaryOperationType(JNumberT, JNumberT)
        def f1(ctx: MorphContext): F1 = CF1P("builtin::unary::neg") {
          case c: DoubleColumn => new DoubleFrom.D(c, doubleIsDefined, -_)
          case c: LongColumn => new LongFrom.L(c, n => true, -_)
          case c: NumColumn => new NumFrom.N(c, n => true, -_)
        }

        def spec[A <: SourceType](ctx: MorphContext): TransSpec[A] => TransSpec[A] = {
          transSpec => trans.Map1(transSpec, f1(ctx))
        }
      }
    }
  }
}
