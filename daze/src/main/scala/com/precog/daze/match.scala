package com.precog
package daze

import bytecode._
import yggdrasil._

trait MatchAlgebra extends OperationsAPI with Instructions {
  import instructions._
  
  def resolveMatch(spec: MatchSpec): PartialFunction[SValue, SValue] = spec match {
    case mal.Actual => { case x => x }
    
    case mal.Op1(parent, op) =>
      pfCompose(resolveUnaryOperation(op), resolveMatch(parent))
    
    // TODO generalize to all statically singleton sets
    case mal.Op2Single(parent, value, op, left) => {
      val f = resolveBinaryOperation(op)
      val pf = resolveMatch(parent)
      
      if (left) {
        {
          case sv if pf.isDefinedAt(sv) && f.isDefinedAt((pf(sv), value)) =>
            f((pf(sv), value))
        }
      } else {
        {
          case sv if pf.isDefinedAt(sv) && f.isDefinedAt((value, pf(sv))) =>
            f((value, pf(sv)))
        }
      }
    }
    
    case mal.Op2Multi(parent1, parent2, op) => {
      val f = resolveBinaryOperation(op)
      val pf1 = resolveMatch(parent1)
      val pf2 = resolveMatch(parent2)
      
      {
        case sv if pf1.isDefinedAt(sv) && pf2.isDefinedAt(sv) && f.isDefinedAt((pf1(sv), pf2(sv))) =>
          f((pf1(sv), pf2(sv)))
      }
    }
    
    case mal.Filter(parent1, parent2) => {
      val pf1 = resolveMatch(parent1)
      val pf2 = resolveMatch(parent2)
      
      {
        case sv if pf1.isDefinedAt(sv) && pf2.isDefinedAt(sv) && pf2(sv) == SBoolean(true) =>
          pf1(sv)
      }
    }
  }
  
  def resolveUnaryOperation(op: UnaryOperation): PartialFunction[SValue, SValue]
  
  def resolveBinaryOperation(op: BinaryOperation): PartialFunction[(SValue, SValue), SValue]
  
  private def pfCompose[A, B, C](left: PartialFunction[B, C], right: PartialFunction[A, B]): PartialFunction[A, C] = {
    case a if right.isDefinedAt(a) && left.isDefinedAt(right(a)) => left(right(a))
  }
  
  
  case class Match(spec: MatchSpec, set: Dataset[SValue])
  
  sealed trait MatchSpec
  
  object mal {
    case object Actual extends MatchSpec
    case class Op1(parent: MatchSpec, op: UnaryOperation) extends MatchSpec
    case class Op2Single(parent: MatchSpec, value: SValue, op: BinaryOperation, left: Boolean) extends MatchSpec
    case class Op2Multi(parent1: MatchSpec, parent2: MatchSpec, op: BinaryOperation) extends MatchSpec
    case class Filter(target: MatchSpec, boolean: MatchSpec) extends MatchSpec
  }
}
