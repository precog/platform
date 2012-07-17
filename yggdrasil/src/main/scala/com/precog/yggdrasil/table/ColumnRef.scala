package com.precog.yggdrasil
package table

import blueeyes.json.JPath
import blueeyes.json.JPath._
import com.precog.common.Path
import scalaz.syntax.semigroup._
import scalaz.syntax.order._

case class ColumnRef(selector: JPath, ctype: CType) {
  type CA = ctype.CA
}

object ColumnRef {
  implicit object order extends scalaz.Order[ColumnRef] {
    def order(r1: ColumnRef, r2: ColumnRef): scalaz.Ordering = {
      (r1.selector ?|? r2.selector) |+| (r1.ctype ?|? r2.ctype)
    }
  }

  implicit val ordering: scala.math.Ordering[ColumnRef]  = order.toScalaOrdering
}


// vim: set ts=4 sw=4 et:
