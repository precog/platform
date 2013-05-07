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

sealed trait IdentityPolicy
object IdentityPolicy {
  sealed trait Retain extends IdentityPolicy
  object Retain {
    /** Right IDs are discarded, left IDs are kept, in order. */
    case object Left extends Retain

    /** Left IDs are discarded, right IDs are kept, in order. */
    case object Right extends Retain

    /**
     * All IDs are kept. Prefix first, then remaining left IDs, then remaining
     * right IDs. The result is in order of the prefix/key.
     *
     * This should also be used in Morph1 to indicate the IDs are retained.
     *
     * TODO: Much like join, custom Morph2's should be allowed to specify order
     *       after the join.
     */
    case object Merge extends Retain

    /**
     * Both IDs are kept, with the left sides first. The left IDs remain in
     * order.
     */
    case object Cross extends Retain
  }
  
  /** A new single column of IDs are synthesized and all other IDs are discarded. */
  case object Synthesize extends IdentityPolicy

  /** All IDs are discarded. */
  case object Strip extends IdentityPolicy

  /** Both identity policies are adhered to, and then concatenated. */
  case class Product(left: IdentityPolicy, right: IdentityPolicy) extends IdentityPolicy
}

trait FunctionLike {
  val namespace: Vector[String]
  val name: String
  val opcode: Int
  val rowLevel: Boolean
  val deprecation: Option[String] = None
  lazy val fqn = if (namespace.isEmpty) name else namespace.mkString("", "::", "::") + name
  override def toString = "[0x%06x]".format(opcode) + fqn
}

trait Morphism1Like extends FunctionLike {
  val tpe: UnaryOperationType
  val isInfinite: Boolean = false
  val idPolicy: IdentityPolicy = IdentityPolicy.Strip      // TODO remove this default
}

object Morphism1Like {
  def unapply(m: Morphism1Like): Option[(Vector[String], String, Int, UnaryOperationType)] =
    Some(m.namespace, m.name, m.opcode, m.tpe)
}

trait Morphism2Like extends FunctionLike {
  val tpe: BinaryOperationType
  val idPolicy: IdentityPolicy = IdentityPolicy.Strip      // TODO remove this default
}

object Morphism2Like {
  def unapply(m: Morphism2Like): Option[(Vector[String], String, Int, BinaryOperationType)] =
    Some(m.namespace, m.name, m.opcode, m.tpe)
}

trait Op1Like extends FunctionLike {
  val tpe: UnaryOperationType
}

object Op1Like {
  def unapply(op1: Op1Like): Option[(Vector[String], String, Int, UnaryOperationType)] =
    Some(op1.namespace, op1.name, op1.opcode, op1.tpe)
}

trait Op2Like extends FunctionLike {
  val tpe: BinaryOperationType
}

object Op2Like {
  def unapply(op2: Op2Like): Option[(Vector[String], String, Int, BinaryOperationType)] =
    Some(op2.namespace, op2.name, op2.opcode, op2.tpe)
}

trait ReductionLike extends FunctionLike {
  val tpe: UnaryOperationType
}

object ReductionLike {
  def unapply(red: ReductionLike): Option[(Vector[String], String, Int)] = Some(red.namespace, red.name, red.opcode)
}

trait Library {
  type Morphism1 <: Morphism1Like
  type Morphism2 <: Morphism2Like
  type Op1 <: Op1Like
  type Op2 <: Op2Like
  type Reduction <: ReductionLike
  
  def expandGlob: Morphism1

  def libMorphism1: Set[Morphism1]
  def libMorphism2: Set[Morphism2]
  def lib1: Set[Op1] 
  def lib2: Set[Op2]
  def libReduction: Set[Reduction]
}

