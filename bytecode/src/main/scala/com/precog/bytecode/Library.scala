package com.precog
package bytecode

trait FunctionLike {
  val namespace: Vector[String]
  val name: String
  val opcode: Int
  lazy val fqn = if (namespace.isEmpty) name else namespace.mkString("", "::", "::") + name
  override def toString = "[0x%06x]".format(opcode) + fqn
}

trait Morphism1Like extends FunctionLike {
  val tpe: UnaryOperationType
  val retainIds: Boolean = false
}

object Morphism1Like {
  def unapply(m: Morphism1Like): Option[(Vector[String], String, Int, UnaryOperationType)] =
    Some(m.namespace, m.name, m.opcode, m.tpe)
}

trait Morphism2Like extends FunctionLike {
  val tpe: BinaryOperationType
  val retainIds: Boolean = false
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

