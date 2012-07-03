package com.precog.yggdrasil

object Schema {
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

  def subsumes(jtpe : JType, ctpe : CType) : Boolean = (jtpe, ctpe) match {
    case (JNumberT, CLong) => true
    case (JNumberT, CDouble) => true
    case (JNumberT, CDecimalArbitrary) => true

    case (JTextT, CStringFixed(_)) => true
    case (JTextT, CStringArbitrary) => true

    case (JBooleanT,  CBoolean) => true

    case (JNullT, CNull) => true
    case (JNullT, CEmptyArray) => true
    case (JNullT, CEmptyObject) => true

    case (JUnionT(ljtpe, rjtpe), ctpe) => subsumes(ljtpe, ctpe) || subsumes(rjtpe, ctpe)

    case _ => false
  }
}
