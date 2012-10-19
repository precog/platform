package com.precog
package daze

import com.precog.yggdrasil.TransSpecModule

import com.precog.bytecode._

trait EvaluatorMethods[M[+_]] extends Instructions with UnaryLib[M] with TransSpecModule with InfixLib[M] {
  import instructions._
  import trans._

  def op1(op: UnaryOperation): Op1 = op match {
    case BuiltInFunction1Op(op1) => op1
    
    case instructions.New | instructions.WrapArray => sys.error("assertion error")
    
    case Comp => Unary.Comp
    case Neg => Unary.Neg
  }

  def transFromBinOp[A <: SourceType](op: BinaryOperation)(left: TransSpec[A], right: TransSpec[A]): TransSpec[A] = op match {
    case Eq => trans.Equal(left, right)
    case NotEq => trans.Map1(trans.Equal(left, right), op1(Comp).f1)
    case instructions.WrapObject => WrapObjectDynamic(left, right)
    case JoinObject => InnerObjectConcat(left, right)
    case JoinArray => ArrayConcat(left, right)
    case instructions.ArraySwap => sys.error("nothing happens")
    case DerefObject => DerefObjectDynamic(left, right)
    case DerefMetadata => sys.error("cannot do a dynamic metadata deref")
    case DerefArray => DerefArrayDynamic(left, right)
    case _ => trans.Map2(left, right, op2ForBinOp(op).get.f2)     // if this fails, we're missing a case above
  }
}
