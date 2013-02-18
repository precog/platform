package com.precog.yggdrasil
package util

import com.precog.common._
import com.precog.yggdrasil.table._
import com.precog.common.json._

import org.joda.time.DateTime

import scala.annotation.tailrec
import scala.{ specialized => spec }

/**
 * Represents the result of an ordering, but with the possibility that no
 * ordering exists. This is isomorphic to Option[Ordering] (and a partial
 * ordering), but I wanted to ensure we weren't allocating objects when
 * returning results.
 */
sealed abstract class MaybeOrdering(val toInt: Int) {
  val toScalazOrdering: scalaz.Ordering = scalaz.Ordering.fromInt(toInt)
  def complement: MaybeOrdering
}

object MaybeOrdering {
  case object Lt extends MaybeOrdering(-1) { def complement = Gt }
  case object Gt extends MaybeOrdering(1) { def complement = Lt }
  case object Eq extends MaybeOrdering(0) { def complement = Eq }
  case object NoComp extends MaybeOrdering(0) { def complement = NoComp }

  def fromInt(n: Int): MaybeOrdering = if (n < 0) Lt else if (n == 0) Eq else Gt
}

trait CPathComparator { self =>
  def compare(row1: Int, row2: Int, indices: Array[Int]): MaybeOrdering

  def swap: CPathComparator = new CPathComparator {
    def compare(row1: Int, row2: Int, indices: Array[Int]): MaybeOrdering =
      self.compare(row2, row1, indices).complement
  }

  def complement: CPathComparator = new CPathComparator {
    def compare(row1: Int, row2: Int, indices: Array[Int]): MaybeOrdering =
      self.compare(row1, row2, indices).complement
  }
}

object CPathComparator {
  import MaybeOrdering._
  import ExtraOrders._

  def apply[@spec(Boolean, Long, Double, AnyRef) A,
            @spec(Boolean, Long, Double, AnyRef) B](
      lCol: Int => A, rCol: Int => B)(implicit order: HetOrder[A, B]) = {
    new CPathComparator {
      def compare(r1: Int, r2: Int, i: Array[Int]) =
        MaybeOrdering.fromInt(order.compare(lCol(r1), rCol(r2)))
    }
  }

  def apply(lPath: CPath, lCol: Column, rPath: CPath, rCol: Column): CPathComparator = (lCol, rCol) match {
    case (lCol: BoolColumn, rCol: BoolColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: LongColumn, rCol: LongColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: LongColumn, rCol: DoubleColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: LongColumn, rCol: NumColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: DoubleColumn, rCol: LongColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: DoubleColumn, rCol: DoubleColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: DoubleColumn, rCol: NumColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: NumColumn, rCol: LongColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: NumColumn, rCol: DoubleColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: NumColumn, rCol: NumColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: StrColumn, rCol: StrColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: DateColumn, rCol: DateColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: HomogeneousArrayColumn[_], rCol: HomogeneousArrayColumn[_]) => CPathComparator(lPath, lCol, rPath, rCol)
    case (lCol: HomogeneousArrayColumn[_], rCol) => CPathComparator(lPath, lCol, rPath, rCol)
    case (lCol, rCol: HomogeneousArrayColumn[_]) => CPathComparator(rPath, rCol, lPath, lCol).swap
    case (lCol, rCol) =>
      val ordering = MaybeOrdering.fromInt {
        implicitly[scalaz.Order[CType]].apply(lCol.tpe, rCol.tpe).toInt
      }
      new CPathComparator {
        def compare(r1: Int, r2: Int, indices: Array[Int]) = ordering
      }
  }

  def apply(lPath: CPath, lCol: HomogeneousArrayColumn[_], rPath: CPath, rCol: HomogeneousArrayColumn[_]): CPathComparator = {
    (lCol.leafTpe, rCol.leafTpe) match {
      case (CLong, CLong) => new ArrayCPathComparator[Long, Long](lPath, lCol, rPath, rCol)
      case (CLong, CDouble) => new ArrayCPathComparator[Long, Double](lPath, lCol, rPath, rCol)
      case (CLong, CNum) => new ArrayCPathComparator[Long, BigDecimal](lPath, lCol, rPath, rCol)
      case (CDouble, CLong) => new ArrayCPathComparator[Double, Long](lPath, lCol, rPath, rCol)
      case (CDouble, CDouble) => new ArrayCPathComparator[Double, Double](lPath, lCol, rPath, rCol)
      case (CDouble, CNum) => new ArrayCPathComparator[Double, BigDecimal](lPath, lCol, rPath, rCol)
      case (CNum, CLong) => new ArrayCPathComparator[BigDecimal, Long](lPath, lCol, rPath, rCol)
      case (CNum, CDouble) => new ArrayCPathComparator[BigDecimal, Double](lPath, lCol, rPath, rCol)
      case (CNum, CNum) => new ArrayCPathComparator[BigDecimal, BigDecimal](lPath, lCol, rPath, rCol)
      case (CBoolean, CBoolean) => new ArrayCPathComparator[Boolean, Boolean](lPath, lCol, rPath, rCol)
      case (CString, CString) => new ArrayCPathComparator[String, String](lPath, lCol, rPath, rCol)
      case (CDate, CDate) => new ArrayCPathComparator[DateTime, DateTime](lPath, lCol, rPath, rCol)
      case (tpe1, tpe2) =>
        val ordering = MaybeOrdering.fromInt(implicitly[scalaz.Order[CType]].apply(lCol.tpe, rCol.tpe).toInt)
        new CPathComparator with ArrayCPathComparatorSupport {
          val lMask = makeMask(lPath)
          val rMask = makeMask(rPath)
          val lSelector = new ArraySelector()(tpe1.manifest)
          val rSelector = new ArraySelector()(tpe2.manifest)

          def compare(r1: Int, r2: Int, indices: Array[Int]): MaybeOrdering = {
            val lPluckable = lSelector.canPluck(lCol(r1), indices, lMask)
            val rPluckable = rSelector.canPluck(rCol(r2), indices, rMask)
            if (lPluckable && rPluckable) {
              ordering
            } else if (lPluckable) {
              Gt
            } else if (rPluckable) {
              Lt
            } else {
              Eq
            }
          }
        }
    }
  }

  def apply(lPath: CPath, lCol: HomogeneousArrayColumn[_], rPath: CPath, rCol: Column): CPathComparator = {
    (lCol.leafTpe, rCol) match {
      case (CLong, rCol: LongColumn) => new HalfArrayCPathComparator[Long, Long](lPath, lCol, rCol(_))
      case (CLong, rCol: DoubleColumn) => new HalfArrayCPathComparator[Long, Double](lPath, lCol, rCol(_))
      case (CLong, rCol: NumColumn) => new HalfArrayCPathComparator[Long, BigDecimal](lPath, lCol, rCol(_))
      case (CDouble, rCol: LongColumn) => new HalfArrayCPathComparator[Double, Long](lPath, lCol, rCol(_))
      case (CDouble, rCol: DoubleColumn) => new HalfArrayCPathComparator[Double, Double](lPath, lCol, rCol(_))
      case (CDouble, rCol: NumColumn) => new HalfArrayCPathComparator[Double, BigDecimal](lPath, lCol, rCol(_))
      case (CNum, rCol: LongColumn) => new HalfArrayCPathComparator[BigDecimal, Long](lPath, lCol, rCol(_))
      case (CNum, rCol: DoubleColumn) => new HalfArrayCPathComparator[BigDecimal, Double](lPath, lCol, rCol(_))
      case (CNum, rCol: NumColumn) => new HalfArrayCPathComparator[BigDecimal, BigDecimal](lPath, lCol, rCol(_))
      case (CBoolean, rCol: BoolColumn) => new HalfArrayCPathComparator[Boolean, Boolean](lPath, lCol, rCol(_))
      case (CString, rCol: StrColumn) => new HalfArrayCPathComparator[String, String](lPath, lCol, rCol(_))
      case (CDate, rCol: DateColumn) => new HalfArrayCPathComparator[DateTime, DateTime](lPath, lCol, rCol(_))
      case (tpe1, _) =>
        val ordering = MaybeOrdering.fromInt(implicitly[scalaz.Order[CType]].apply(tpe1, rCol.tpe).toInt)
        new CPathComparator with ArrayCPathComparatorSupport {
          val mask = makeMask(lPath)
          val selector = new ArraySelector()(tpe1.manifest)
          def compare(r1: Int, r2: Int, indices: Array[Int]): MaybeOrdering = {
            if (selector.canPluck(lCol(r1), indices, mask)) {
              ordering
            } else Lt
          }
        }
    }
  }
}

private[yggdrasil] trait ArrayCPathComparatorSupport {
  final def makeMask(path: CPath): Array[Boolean] = {
    val indexNodes = path.nodes filter {
      case CPathIndex(_) => true
      case CPathArray => true
      case _ => false
    }

    val mask = new Array[Boolean](indexNodes.size)
    indexNodes.zipWithIndex foreach {
      case (CPathArray, i) => mask(i) = true
      case _ =>
    }
    mask
  }
}

/**
 * A non-boxing CPathComparator where the left-side is a homogeneous array and
 * the right side is not.
 */
private[yggdrasil] final class HalfArrayCPathComparator[@spec(Boolean, Long, Double) A, @spec(Boolean, Long, Double) B](
    lPath: CPath, lCol: HomogeneousArrayColumn[_], rCol: Int => B)(implicit
    ma: Manifest[A], ho: HetOrder[A, B]) extends CPathComparator with ArrayCPathComparatorSupport {

  final lazy val lMask: Array[Boolean] = makeMask(lPath)
  
  val lSelector = new ArraySelector[A]

  def compare(r1: Int, r2: Int, indices: Array[Int]): MaybeOrdering = {
    import MaybeOrdering._

    val left = lCol(r1)

    val lPluckable = lSelector.canPluck(left, indices, lMask)

    if (lPluckable) {
      val a = lSelector.pluck(left, indices, lMask)
      val cmp = ho.compare(a, rCol(r2))
      if (cmp < 0) Lt else if (cmp == 0) Eq else Gt
    } else {
      Lt
    }
  }
}

/**
 * A non-boxing CPathComparator for homogeneous arrays.
 */
private[yggdrasil] final class ArrayCPathComparator[@spec(Boolean, Long, Double) A, @spec(Boolean, Long, Double) B](
    lPath: CPath, lCol: HomogeneousArrayColumn[_], rPath: CPath, rCol: HomogeneousArrayColumn[_])(implicit
    ma: Manifest[A], mb: Manifest[B], ho: HetOrder[A, B]) extends CPathComparator with ArrayCPathComparatorSupport {

  // FIXME: These are lazy to get around a bug in @spec. We can probably remove
  // this in 2.10.

  final lazy val lMask: Array[Boolean] = makeMask(lPath)
  final lazy val rMask: Array[Boolean] = makeMask(rPath)

  val lSelector = new ArraySelector[A]
  val rSelector = new ArraySelector[B]

  def compare(r1: Int, r2: Int, indices: Array[Int]): MaybeOrdering = {
    import MaybeOrdering._

    val left = lCol(r1)
    val right = rCol(r2)

    val lPluckable = lSelector.canPluck(left, indices, lMask)
    val rPluckable = rSelector.canPluck(right, indices, rMask)

    if (lPluckable) {
      if (rPluckable) {
        val a = lSelector.pluck(left, indices, lMask)
        val b = rSelector.pluck(right, indices, rMask)
        val cmp = ho.compare(a, b)
        if (cmp < 0) Lt else if (cmp == 0) Eq else Gt
      } else {
        Gt
      }
    } else if (rPluckable) {
      Lt
    } else {
      NoComp
    }
  }
}

/**
 * ArraySelector provides a non-boxing way of accessing the leaf elements in a
 * bunch of nested arrays.
 */
private[yggdrasil] final class ArraySelector[@spec(Boolean, Long, Double) A](implicit m: Manifest[A]) {
  private val am = m.arrayManifest

  def canPluck(a: Array[_], indices: Array[Int], mask: Array[Boolean]): Boolean = {
    var arr: Array[_] = a
    var i = 0
    while (i < mask.length) {
      if (mask(i)) {
        if (am.erasure.isInstance(arr)) {
          return indices(i) < arr.length
        } else {
          if (indices(i) < arr.length) {
            arr = arr(indices(i)).asInstanceOf[Array[_]]
          } else {
            return false
          }
        }
      }

      i += 1
    }

    return false
  }

  def pluck(a: Array[_], indices: Array[Int], mask: Array[Boolean]): A = {
    var arr: Array[_] = a
    var i = 0

    while (i < mask.length) {
      if (mask(i)) {
        if (am.erasure.isInstance(arr)) {
          val sarr = arr.asInstanceOf[Array[A]]
          return sarr(indices(i))
        } else {
          arr = arr(indices(i)).asInstanceOf[Array[_]]
        }
      }

      i += 1
    }

    sys.error("This shouldn't happens and indicates a problem with canPluck")
  }
}

