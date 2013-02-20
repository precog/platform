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
package com.precog.niflheim

import com.precog.util._
import com.precog.util.BitSetUtil.Implicits._
import com.precog.common._

import scala.{ specialized => sepc }

import scalaz._

trait Reduction[A] {
  def semigroup: Semigroup[A]
  def reduce(segment: Segment, mask: Option[BitSet] = None): Option[A]
}

object Reductions {
  private def bitsOrBust[A](defined: BitSet, mask: Option[BitSet])(f: BitSet => A): Option[A] = {
    val bits = mask map (_ & defined) getOrElse defined
    if (bits.isEmpty) None else Some(f(bits))
  }

  object count extends Reduction[Long] {
    object semigroup extends Semigroup[Long] {
      def append(x: Long, y: => Long): Long = x + y
    }

    def reduce(segment: Segment, mask: Option[BitSet]): Option[Long] =
      bitsOrBust(segment.defined, mask)(_.cardinality.toLong)
  }

  object min extends Reduction[BigDecimal] {
    object semigroup extends Semigroup[BigDecimal] {
      def append(x: BigDecimal, y: => BigDecimal): BigDecimal = x min y
    }

    def reduce(segment: Segment, mask: Option[BitSet]): Option[BigDecimal] = segment match {
      case seg: ArraySegment[a] =>
        seg.values match {
          case values: Array[Long] =>
            bitsOrBust(seg.defined, mask) { bits =>
              var min = Long.MaxValue
              bits.foreach { row =>
                if (values(row) < min) {
                  min = values(row)
                }
              }
              BigDecimal(min)
            }

          case values: Array[Double] =>
            bitsOrBust(seg.defined, mask) { bits =>
              var min = Double.PositiveInfinity
              bits.foreach { row =>
                if (values(row) < min) {
                  min = values(row)
                }
              }
              BigDecimal(min)
            }

          case values: Array[BigDecimal] =>
            bitsOrBust(seg.defined, mask) { bits =>
              var min: BigDecimal = null
              bits.foreach { row =>
                if (min == null || values(row) < min) {
                  min = values(row)
                }
              }
              min
            }

          case _ =>
            None
        }

      case _ =>
        None
    }
  }

  object max extends Reduction[BigDecimal] {
    object semigroup extends Semigroup[BigDecimal] {
      def append(x: BigDecimal, y: => BigDecimal): BigDecimal = x max y
    }

    def reduce(segment: Segment, mask: Option[BitSet]): Option[BigDecimal] = segment match {
      case seg: ArraySegment[a] =>
        seg.values match {
          case values: Array[Long] =>
            bitsOrBust(seg.defined, mask) { bits =>
              var min = Long.MinValue
              bits.foreach { row =>
                if (values(row) > min) {
                  min = values(row)
                }
              }
              BigDecimal(min)
            }

          case values: Array[Double] =>
            bitsOrBust(seg.defined, mask) { bits =>
              var min = Double.NegativeInfinity
              bits.foreach { row =>
                if (values(row) > min) {
                  min = values(row)
                }
              }
              BigDecimal(min)
            }

          case values: Array[BigDecimal] =>
            bitsOrBust(seg.defined, mask) { bits =>
              var min: BigDecimal = null
              bits.foreach { row =>
                if (min == null || values(row) > min) {
                  min = values(row)
                }
              }
              min
            }

          case _ =>
            None
        }

      case _ =>
        None
    }
  }
}
