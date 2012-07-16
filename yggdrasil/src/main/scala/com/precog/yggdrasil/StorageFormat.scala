package com.precog.yggdrasil

sealed trait StorageFormat {
  def min(i: Int): Int
  val isFixed: Boolean
}

case object LengthEncoded extends StorageFormat {
  def min(i: Int) = i
  final val isFixed = false
}

case class  FixedWidth(width: Int) extends StorageFormat {
  def min(i: Int) = width min i
  final val isFixed = true
}


// vim: set ts=4 sw=4 et:
