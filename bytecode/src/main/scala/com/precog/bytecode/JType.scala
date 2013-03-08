package com.precog.bytecode

sealed trait JType {
  def |(jtype: JType) = JUnionT(this, jtype)
}

sealed trait JPrimitiveType extends JType

case object JNumberT extends JPrimitiveType
case object JTextT extends JPrimitiveType
case object JBooleanT extends JPrimitiveType
case object JNullT extends JPrimitiveType

case object JDateT extends JPrimitiveType
case object JPeriodT extends JPrimitiveType

sealed trait JArrayT extends JType
case class JArrayHomogeneousT(jType: JType) extends JArrayT
case class JArrayFixedT(elements: Map[Int, JType]) extends JArrayT
case object JArrayUnfixedT extends JArrayT

sealed trait JObjectT extends JType
case class JObjectFixedT(fields: Map[String, JType]) extends JObjectT
case object JObjectUnfixedT extends JObjectT

case class JUnionT(left: JType, right: JType) extends JType {
  override def toString = {
    if (this == JType.JUniverseT)
      "JUniverseT"
    else
      "JUnionT" + "(" + left + ", " + right + ")"
  }
}

object JType {
  // TODO JArrayHomogeneousT can't go in here. Is this just used for tests?
  val JPrimitiveUnfixedT = JNumberT | JTextT | JBooleanT | JNullT | JDateT | JPeriodT
  // val JUnfixedT = JPrimitiveUnfixedT | JObjectUnfixedT | JArrayUnfixedT
  // val JUniverseT = JUnionT(JUnionT(JUnionT(JUnionT(JUnionT(JUnionT(JUnionT(JNumberT, JTextT), JBooleanT), JNullT), JDateT), JPeriodT), JObjectUnfixedT), JArrayUnfixedT)
  val JUniverseT = JPrimitiveUnfixedT | JObjectUnfixedT | JArrayUnfixedT
}

case class UnaryOperationType(arg: JType, result: JType)
case class BinaryOperationType(arg0: JType, arg1: JType, result: JType)
