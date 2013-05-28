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

import yggdrasil._
import bytecode.{ BinaryOperationType, JNumberT, JBooleanT, JTextT, Library, Instructions }

trait OpFinderModule[M[+_]] extends Instructions with TableModule[M] with TableLibModule[M] {
  import instructions._

  trait OpFinder {
    def op1ForUnOp(op: UnaryOperation): library.Op1
    def op2ForBinOp(op: BinaryOperation): Option[library.Op2] 
  }
}

trait StdLibOpFinderModule[M[+_]] extends Instructions with StdLibModule[M] with OpFinderModule[M] {
  import instructions._
  import library._

  trait StdLibOpFinder extends OpFinder {
    override def op1ForUnOp(op: UnaryOperation) = op match {
      case BuiltInFunction1Op(op1) => op1
      case New | WrapArray => sys.error("assertion error")
      case Comp => Unary.Comp
      case Neg => Unary.Neg
    }

    override def op2ForBinOp(op: BinaryOperation) = {
      import instructions._
      
      op match {
        case BuiltInFunction2Op(op2) => Some(op2)
        case Add => Some(Infix.Add)
        case Sub => Some(Infix.Sub)
        case Mul => Some(Infix.Mul)
        case Div => Some(Infix.Div)
        case Mod => Some(Infix.Mod)
        case Pow => Some(Infix.Pow)
        
        case Lt => Some(Infix.Lt)
        case LtEq => Some(Infix.LtEq)
        case Gt => Some(Infix.Gt)
        case GtEq => Some(Infix.GtEq)
        
        case Eq | instructions.NotEq => None
        
        case Or => Some(Infix.Or)
        case And => Some(Infix.And)
        
        case WrapObject | JoinObject |
             JoinArray | ArraySwap | DerefMetadata |
             DerefObject | DerefArray => None
      }
    }
  }
}


// vim: set ts=4 sw=4 et:
