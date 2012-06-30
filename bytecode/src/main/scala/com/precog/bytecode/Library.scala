package com.precog
package bytecode

trait MorphismLike {
  val namespace: Vector[String]
  val name: String
  val opcode: Int

  lazy val fqn = if (namespace.isEmpty) name else namespace.mkString("", "::", "::") + name
  override def toString = "[0x%06x]".format(opcode) + fqn
}

trait Op1Like {
  val namespace: Vector[String]
  val name: String
  val opcode: Int

  def fqn: String
  override def toString = "[0x%06x]".format(opcode) + fqn
}

trait Op2Like {
  val namespace: Vector[String]
  val name: String
  val opcode: Int

  def fqn: String
  override def toString = "[0x%06x]".format(opcode) + fqn
}

trait ReductionLike {
  val namespace: Vector[String]
  val name: String
  val opcode: Int

  def fqn: String
  override def toString = "[0x%06x]".format(opcode) + fqn
}

trait Library {
  type Morphism <: MorphismLike
  type Op1 <: Op1Like with Morphism
  type Op2 <: Op2Like with Morphism
  type Reduction <: ReductionLike with Morphism

  def libMorphism: Set[Morphism]
  def lib1: Set[Op1] 
  def lib2: Set[Op2]
  def libReduction: Set[Reduction]
}

