/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.yggdrasil
package table

import blueeyes.json.JPath
import blueeyes.json.JPath._
import com.precog.common.Path
import scalaz.syntax.semigroup._
import scalaz.syntax.order._

case class ColumnRef(selector: JPath, ctype: CType)

object ColumnRef {
  implicit object order extends scalaz.Order[ColumnRef] {
    def order(r1: ColumnRef, r2: ColumnRef): scalaz.Ordering = {
      (r1.selector ?|? r2.selector) |+| (r1.ctype ?|? r2.ctype)
    }
  }

  implicit val ordering: scala.math.Ordering[ColumnRef]  = order.toScalaOrdering
}
