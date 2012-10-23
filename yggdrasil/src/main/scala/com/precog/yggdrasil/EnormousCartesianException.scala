package com.precog.yggdrasil

case class EnormousCartesianException(left: TableSize, right: TableSize) extends RuntimeException {
  override def getMessage =
    "cannot evaluate cartesian of sets with size %s and %s".format(left, right)
}
