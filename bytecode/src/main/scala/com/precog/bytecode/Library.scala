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
  val tpe: UnaryOperationType

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

