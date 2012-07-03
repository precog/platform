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

trait Op1Like {
  val namespace: Vector[String]
  val name: String
  val opcode: Int

  lazy val fqn = if (namespace.isEmpty) name else namespace.mkString("", "::", "::") + name
  override def toString = "[0x%06x]".format(opcode) + fqn
}

trait Op2Like {
  val namespace: Vector[String]
  val name: String
  val opcode: Int

  lazy val fqn = if (namespace.isEmpty) name else namespace.mkString("", "::", "::") + name
  override def toString = "[0x%06x]".format(opcode) + fqn
}

trait ReductionLike {
  val namespace: Vector[String]
  val name: String
  val opcode: Int

  lazy val fqn = if (namespace.isEmpty) name else namespace.mkString("", "::", "::") + name
  override def toString = "[0x%06x]".format(opcode) + fqn
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

