package com.precog
package bytecode

trait BuiltInRed {
  def namespace: Vector[String]
  def name: String
  def opcode: Int

  lazy val fqn = if (namespace.isEmpty) name else namespace.mkString("", "::", "::") + name
  override def toString = "[0x%06x]".format(opcode) + fqn
}

trait BuiltInFunc1 {
  def namespace: Vector[String]
  def name: String
  def opcode: Int
  def isOperation: Boolean

  lazy val fqn = if (namespace.isEmpty) name else namespace.mkString("", "::", "::") + name
  override def toString = "[0x%06x]".format(opcode) + fqn
}

trait BuiltInFunc2 {
  def namespace: Vector[String]
  def name: String
  def opcode: Int
  def isOperation: Boolean

  lazy val fqn = if (namespace.isEmpty) name else namespace.mkString("", "::", "::") + name
  override def toString = "[0x%06x]".format(opcode) + fqn
}

trait Library {
  type BIR <: BuiltInRed
  type BIF1 <: BuiltInFunc1
  type BIF2 <: BuiltInFunc2

  def libReduct: Set[BIR]
  def lib1: Set[BIF1] 
  def lib2: Set[BIF2]
}


