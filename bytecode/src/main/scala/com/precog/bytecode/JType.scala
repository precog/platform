package com.precog.bytecode

sealed trait JType {
  def |(jtype: JType) = JUnionT(this, jtype)
}

sealed trait JPrimitiveType extends JType

case object JNumberT extends JPrimitiveType
case object JTextT extends JPrimitiveType
case object JBooleanT extends JPrimitiveType
case object JNullT extends JPrimitiveType

sealed trait JArrayT extends JType
case class JArrayFixedT(elements: Map[Int, JType]) extends JArrayT
case object JArrayUnfixedT extends JArrayT

sealed trait JObjectT extends JType
case class JObjectFixedT(fields: Map[String, JType]) extends JObjectT
case object JObjectUnfixedT extends JObjectT

case class JUnionT(left: JType, right: JType) extends JType

object JType {
  val JPrimitiveUnfixedT = JNumberT | JTextT | JBooleanT | JNullT
  val JUnfixedT = JPrimitiveUnfixedT | JObjectUnfixedT | JArrayUnfixedT
}
