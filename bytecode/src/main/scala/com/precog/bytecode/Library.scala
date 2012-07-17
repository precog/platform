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
package bytecode

sealed trait Arity {
  val ordinal: Int
}


object Arity {
  case object One extends Arity {
    val ordinal = 1
  }
  case object Two extends Arity {
    val ordinal = 2
  }
}

trait MorphismLike {
  val namespace: Vector[String]
  val name: String
  val opcode: Int
  val arity: Arity

  lazy val fqn = if (namespace.isEmpty) name else namespace.mkString("", "::", "::") + name
  override def toString = "[0x%06x]".format(opcode) + fqn
}

object MorphismLike {
  def unapply(m : MorphismLike) : Option[(Vector[String], String, Int, Arity)] = Some(m.namespace, m.name, m.opcode, m.arity)
}

trait Op1Like {
  val namespace: Vector[String]
  val name: String
  val opcode: Int
  val tpe: UnaryOperationType

  def fqn: String
  override def toString = "[0x%06x]".format(opcode) + fqn
}

object Op1Like {
  def unapply(op1 : Op1Like) : Option[(Vector[String], String, Int)] = Some(op1.namespace, op1.name, op1.opcode)
}

trait Op2Like {
  val namespace: Vector[String]
  val name: String
  val opcode: Int
  val tpe: BinaryOperationType

  def fqn: String
  override def toString = "[0x%06x]".format(opcode) + fqn
}

object Op2Like {
  def unapply(op2 : Op2Like) : Option[(Vector[String], String, Int)] = Some(op2.namespace, op2.name, op2.opcode)
}

trait ReductionLike {
  val namespace: Vector[String]
  val name: String
  val opcode: Int

  def fqn: String
  override def toString = "[0x%06x]".format(opcode) + fqn
}

object ReductionLike {
  def unapply(red : ReductionLike) : Option[(Vector[String], String, Int)] = Some(red.namespace, red.name, red.opcode)
}

trait Library {
  type Morphism <: MorphismLike
  type Op1 <: Op1Like
  type Op2 <: Op2Like
  type Reduction <: ReductionLike

  def libMorphism: Set[Morphism]
  def lib1: Set[Op1] 
  def lib2: Set[Op2]
  def libReduction: Set[Reduction]
}

