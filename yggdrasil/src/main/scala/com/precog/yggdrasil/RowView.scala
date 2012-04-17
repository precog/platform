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

trait RowView {
  import RowView._

  // A position is a handle that can be used to reset a view to a given position in the stream. 
  // Positions should as a consequence be used carefully and be unreferenced as soon as possible
  // because holding a position may imply keeping a reference to an indeterminate amount of the stream in memory.
  type Position 

  def position: Position
  def state: State
  def advance(): State
  def reset(position: Position): State

  //def rename(from: CMeta, to: CMeta): RowView

  def compareIdentities(other: RowView, indices: VectorCase[Int]): Ordering = {
    var result: Ordering = EQ
    var i = 0
    while (i < indices.length && (result eq EQ)) {
      val j = indices(i)
      result = longInstance.order(idAt(j), other.idAt(j))
      i += 1
    }

    result
  }

  def compareIdentityPrefix(other: RowView, limit: Int): Ordering = {
    var result: Ordering = EQ
    var i = 0
    while (i < limit && (result eq EQ)) {
      result = longInstance.order(idAt(i), other.idAt(i))
      i += 1
    }

    result
  }

  def compareValues(other: RowView, meta: CMeta*): Ordering = {
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

  protected[yggdrasil] def idCount: Int
  protected[yggdrasil] def columns: Set[CMeta]

  protected[yggdrasil] def idAt(i: Int): Identity
  protected[yggdrasil] def hasValue(meta: CMeta): Boolean
  protected[yggdrasil] def valueAt(meta: CMeta): Any
}

object RowView {
  sealed trait State 
  case object BeforeStart extends State
  case object Data extends State
  case object AfterEnd extends State
}
