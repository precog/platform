package com.precog.common

import scalaz.syntax.semigroup._
import scalaz.syntax.order._

case class ColumnRef(selector: CPath, ctype: CType)

object ColumnRef {
  def identity(ctype: CType) = ColumnRef(CPath.Identity, ctype)

  implicit object order extends scalaz.Order[ColumnRef] {
    def order(r1: ColumnRef, r2: ColumnRef): scalaz.Ordering = {
      (r1.selector ?|? r2.selector) |+| (r1.ctype ?|? r2.ctype)
    }
  }

  implicit val ordering: scala.math.Ordering[ColumnRef] = order.toScalaOrdering
}