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

sealed trait DataRef

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

  def rename[T <: ColumnRef](from: T, to: T): RowView = sys.error("make abstract")

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

  def compareValues(other: RowView, meta: VColumnRef*): Ordering = {
    import VColumnRef.cast
    var result: Ordering = EQ
    var i = 0
    while (i < meta.length && (result eq EQ)) {
      val m = meta(i)
      result = m.ctype match {
        case CBoolean          => 
          val v = valueAt[Boolean](cast[Boolean](m))
          if (v == other.valueAt[Boolean](cast[Boolean](m))) EQ else if (v) GT else LT

        case CInt              => 
          val v = valueAt[Int](cast[Int](m))
          val ov = other.valueAt[Int](cast[Int](m))
          if (v > ov) GT else if (v == ov) EQ else LT

        case CLong             =>  
          val v = valueAt[Long](cast[Long](m))
          val ov = other.valueAt[Long](cast[Long](m))
          if (v > ov) GT else if (v == ov) EQ else LT

        case CFloat            =>  
          val v = valueAt[Float](cast[Float](m))
          val ov = other.valueAt[Float](cast[Float](m))
          if (v > ov) GT else if (v == ov) EQ else LT

        case CDouble           =>  
          val v = valueAt[Double](cast[Double](m))
          val ov = other.valueAt[Double](cast[Double](m))
          if (v > ov) GT else if (v == ov) EQ else LT

        case CStringArbitrary | CStringFixed(_) =>
          val v = valueAt[String](cast[String](m))
          val ov = other.valueAt[String](cast[String](m))
          if (v > ov) GT else if (v == ov) EQ else LT

        case CDecimalArbitrary =>
          val v = valueAt[BigDecimal](cast[BigDecimal](m))
          val ov = other.valueAt[BigDecimal](cast[BigDecimal](m))
          if (v > ov) GT else if (v == ov) EQ else LT

        case CNull | CEmptyArray | CEmptyObject => EQ
      }                                

      i += 1
    }

    result
  }

  protected[yggdrasil] def idCount: Int
  protected[yggdrasil] def columns: Set[VColumnRef]

  protected[yggdrasil] def idAt(i: Int): Identity
  protected[yggdrasil] def hasValue(meta: VColumnRef): Boolean
  protected[yggdrasil] def valueAt[@specialized(Boolean, Int, Long, Float, Double) A](ref: VColumnRef { type CA = A }): A 
  protected[yggdrasil] def dataAt[@specialized(Boolean, Int, Long, Float, Double) A](ref: ColumnRef { type CA = A }): A = {
    ref match {
      case IColumnRef(idx) => idAt(idx).asInstanceOf[A]
      case ref: VColumnRef { type CA = A } => valueAt(ref)
    }
  }
}

object RowView {
  sealed trait State 
  case object BeforeStart extends State
  case object Data extends State
  case object AfterEnd extends State

  //def vCompare[A](id: VColumnId, ctype: CType { type CA = A }, l: RowView, r: RowView) = macro vCompareImpl[A]
}
