package com.precog.yggdrasil

trait Schema {
  sealed trait JType
  
  case object JNumberT extends JType
  case object JTextT extends JType
  case object JBooleanT extends JType
  case object JNullT extends JType
  
  sealed trait JArrayT extends JType
  case class JArrayFixedT(tpe: JType) extends JArrayT
  case object JArrayUnfixedT extends JArrayT

  sealed trait JObjectT extends JType
  case class JObjectFixedT(fields: Map[String, JType]) extends JObjectT
  case object JObjectUnfixedT extends JObjectT

  case class JUnionT(left: JType, right: JType) extends JType
  
  def flattenUnions(tpe: JType): Set[JType] = tpe match {
    case JUnionT(left, right) => flattenUnions(left) ++ flattenUnions(right)
    case t => Set(t)
  }
}
