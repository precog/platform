package com.precog.analytics

sealed trait AccessType

case object READ extends AccessType
case object WRITE extends AccessType
case object SHARE extends AccessType
case object EXPLORE extends AccessType

object AccessType {
  val ALL: Set[AccessType] = Set(READ, WRITE, SHARE, EXPLORE)
}
