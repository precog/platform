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

import com.precog.common.VectorCase

import java.nio._
import scala.annotation.tailrec
import scalaz.Ordering
import scalaz.Ordering._
import scalaz.syntax.std.allV._
import scalaz.syntax.order._
import scalaz.std.anyVal._
import scalaz.std.string._
import scalaz.std.math.bigDecimal._

trait RowState {
  def compareIdentities(other: RowState, indices: VectorCase[Int]): Ordering = {
    var result: Ordering = EQ
    var i = 0
    while (i < indices.length && (result eq EQ)) {
      result = longInstance.order(idAt(i), other.idAt(i))
      i += 1
    }

    result
  }

  def compareValues(other: RowState, meta: CMeta*): Ordering = {
    var result: Ordering = EQ
    var i = 0
    while (i < meta.length && (result eq EQ)) {
      val m = meta(i)
      result = m.ctype match {
        case v @ CBoolean          => v.cast(valueAt(m)) ?|? v.cast(other.valueAt(m))
        case v @ CStringFixed(_)   => v.cast(valueAt(m)) ?|? v.cast(other.valueAt(m))
        case v @ CStringArbitrary  => v.cast(valueAt(m)) ?|? v.cast(other.valueAt(m))
        case v @ CInt              => v.cast(valueAt(m)) ?|? v.cast(other.valueAt(m))
        case v @ CLong             => v.cast(valueAt(m)) ?|? v.cast(other.valueAt(m))
        case v @ CFloat            => v.cast(valueAt(m)) ?|? v.cast(other.valueAt(m))
        case v @ CDouble           => v.cast(valueAt(m)) ?|? v.cast(other.valueAt(m))
        case v @ CDecimalArbitrary => v.cast(valueAt(m)) ?|? v.cast(other.valueAt(m))
        case CNull | CEmptyArray | CEmptyObject => EQ
      }                                

      i += 1
    }

    result
  }

  protected def idAt(i: Int): Identity
  protected def valueAt(meta: CMeta): Any
}
