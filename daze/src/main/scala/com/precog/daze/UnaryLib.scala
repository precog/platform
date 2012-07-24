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

