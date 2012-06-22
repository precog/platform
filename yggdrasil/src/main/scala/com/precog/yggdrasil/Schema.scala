package com.precog.yggdrasil

trait Schema {
  sealed trait SType
  
  case object SNumber extends SType
  case object SText extends SType
  case object SBoolean extends SType
  case object SNull extends SType
  case object SUnknown extends SType          // ahhhhhhhhh!!!!
  
  case class SArray(tpe: SType) extends SType
  case class SObject(fields: Map[String, SType]) extends SType
  case class SUnion(left: SType, right: SType) extends SType
  
  def flattenUnions(tpe: SType): Set[SType] = tpe match {
    case SUnion(left, right) => flattenUnions(left) ++ flattenUnions(right)
    case t => Set(t)
  }
}
