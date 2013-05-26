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
