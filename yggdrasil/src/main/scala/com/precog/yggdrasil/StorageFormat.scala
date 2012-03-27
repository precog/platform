package com.precog.yggdrasil

sealed trait StorageFormat {
  def min(i: Int): Int
}

case object LengthEncoded extends StorageFormat {
  def min(i: Int) = i
}

case class  FixedWidth(width: Int) extends StorageFormat {
  def min(i: Int) = width min i
}


// vim: set ts=4 sw=4 et:
