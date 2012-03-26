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

import bytecode._
import yggdrasil._

trait MatchAlgebra extends OperationsAPI with Instructions with DAG {
  import instructions._
  import dag.Root
  
  def resolveMatch(spec: MatchSpec): PartialFunction[SValue, SValue] = spec match {
    case mal.Actual => { case x => x }
    
    case mal.Op1(parent, op) =>
      pfCompose(resolveUnaryOperation(op), resolveMatch(parent))
    
    // TODO generalize to all statically singleton sets
    case mal.Op2Single(parent, root, op, left) => {
      val Some(value) = root.value
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
  
  
  sealed trait MatchSpec
  
  object mal {
    case class Match(spec: MatchSpec, set: Dataset[SValue])
    
    case object Actual extends MatchSpec
    case class Op1(parent: MatchSpec, op: UnaryOperation) extends MatchSpec
    case class Op2Single(parent: MatchSpec, root: Root, op: BinaryOperation, left: Boolean) extends MatchSpec
    case class Op2Multi(parent1: MatchSpec, parent2: MatchSpec, op: BinaryOperation) extends MatchSpec
    case class Filter(target: MatchSpec, boolean: MatchSpec) extends MatchSpec
  }
}
