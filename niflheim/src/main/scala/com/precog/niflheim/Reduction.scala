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
