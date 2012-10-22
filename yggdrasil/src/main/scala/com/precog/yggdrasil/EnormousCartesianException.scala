package com.precog.yggdrasil

case class EnormousCartesianException(left: Option[Long], right: Option[Long]) extends RuntimeException {
  override def getMessage = {
    val msgM = for (ls <- left; rs <- right)
      yield "cannot evaluate cartesian of sets with size %d and %d".format(ls, rs)
    
    msgM getOrElse "cannot evaluate large cartesian"
  }
}
