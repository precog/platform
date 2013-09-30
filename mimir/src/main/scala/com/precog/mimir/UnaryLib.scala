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
package mimir

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
