package com.precog.yggdrasil.util

import scala.{ specialized => spec }

sealed trait MaybeOrdering { def toScalazOrdering: scalaz.Ordering }
case object Lt extends MaybeOrdering { val toScalazOrdering = scalaz.Ordering.LT }
case object Gt extends MaybeOrdering { val toScalazOrdering = scalaz.Ordering.GT }
case object Eq extends MaybeOrdering { val toScalazOrdering = scalaz.Ordering.EQ }
case object NoComp extends MaybeOrdering { val toScalazOrdering = scalaz.Ordering.EQ }

sealed trait CPathComparator {
  def compare(row1: Int, row2: Int, indices: Array[Int]): MaybeOrdering
}

object CPathComparator {
  implicit object BooleanOrder extends spire.math.Order[Boolean] {
    def eqv(a: Boolean, b: Boolean) = a == b
    def compare(a: Boolean, b: Boolean) = if (a == b) Eq else if (a) Gt else Lt
  }

  implicit object StringOrder extends spire.math.Order[String] {
    def eqv(a: Boolean, b: Boolean) = a == b
    def compare(a: Boolean, b: Boolean) = a compareTo b
  }

  implicit object DateTimeOrder extends spire.math.Order[DateTime] {
    def eqv(a: DateTime, b: DateTime) = compare(a, b) == 0
    def compare(a: DateTime, b: DateTime) = a compareTo b
  }

  def apply[@spec(Boolean, Long, Double, AnyRef) A,
            @spec(Boolean, Long, Double, AnyRef) B](
      lCol: Int => A, rCol: Int => B)(implicit order: HetOrder[A, B]) = {
    new CPathComparator {
      def compare(r1: Int, r2: Int, i: Array[Int]) = order.compare(lCol(r1), rCol(r2))
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
    case (lCol: HomogeneousArrayColumn[_], rCol: HomogeneousArrayColumn[_]) =>
      (lCol.leafTpe, rCol.leafTpe) match {
        case (CLong, CLong) => new ArrayCPathComparator[Long, Long](lPath, lCol, rPath, rCol)
        case (CLong, CDouble) => new ArrayCPathComparator[Long, Double](lPath, lCol, rPath, rCol)
        case (CLong, CNum) => new ArrayCPathComparator[Long, BigDecimal](lPath, lCol, rPath, rCol)
        case (CDouble, CLong) => new ArrayCPathComparator[Double, Long](lPath, lCol, rPath, rCol)
        case (CDouble, CDouble) => new ArrayCPathComparator[Double, Double](lPath, lCol, rPath, rCol)
        case (CDouble, CNum) => new ArrayCPathComparator[Double, BigDecimal](lPath, lCol, rPath, rCol)
        case (CNum, CLong) => new ArrayCPathComparator[Bounded, Long](lPath, lCol, rPath, rCol)
        case (CNum, CDouble) => new ArrayCPathComparator[BigDecimal, Double](lPath, lCol, rPath, rCol)
        case (CNum, CNum) => new ArrayCPathComparator[BigDecimal, BigDecimal](lPath, lCol, rPath, rCol)
        case (CBoolean, CBoolean) => new ArrayCPathComparator[Boolean, Boolean](lPath, lCol, rPath, rCol)
        case (CString, CString) => new ArrayCPathComparator[String, String](lPath, lCol, rPath, rCol)
        case (CDate, CDate) => new ArrayCPathComparator[DateTime, DateTime](lPath, lCol, rPath, rCol)
        case (tpe1, tpe2) =>
          val ordering = implicitly[scalaz.Order[CType]].apply(c1.tpe, c2.tpe).toInt
          new ArrayCPathComparator[Any, Any](lPath, lCol, rPath, rCol)(implicitly, implicitly, new HetOrder[Any, Any] {
            def compare(a: Any, b: Any): Int = ordering
          })
      }
    case (lCol, rCol) =>
      val ordering = implicitly[scalaz.Order[CType]].apply(c1.tpe, c2.tpe).toInt
      new CPathComparator {
        def compare(r1: Int, r2: Int, indices: Array[Int]) = ordering
      }
  }
}

sealed trait HetOrder[@spec(Boolean, Long, Double, AnyRef) A, @spec(Boolean, Long, Double, AnyRef) B] {
  def compare(a: A, b: B): Int
}

trait HetOrderLow {
  implicit def reverse[@spec(Boolean, Long, Double, AnyRef) A, @spec(Boolean, Long, Double, AnyRef) B](
      implicit ho: HetOrder[B, A]) = new HetOrder[B, A] {
    def compare(b: B, a: A) = {
      val cmp = ho.compare(a, b)
      if (cmp < 0) 1 else if (cmp == 0) 0 else -1
    }
  }

  implicit def fromOrder[@spec(Boolean, Long, Double, AnyRef) A](
      implicit o: spire.math.Order[A]) = new HetOrder[A, A] {
    def compare(a: A, b: A) = o.compare(a, b)
  }
}

object HetOrder extends HetOrderLow {
  implicit object LongDoubleOrder extends HetOrder[Long, Double] {
    def compare(a: Long, b: Double): Int = NumericComparisons.order(a, b)
  }

  implicit object LongBigDecimalOrder extends HetOrder[Long, BigDecimal] {
    def compare(a: Long, b: BigDecimal): Int = NumericComparisons.order(a, b)
  }

  implicit object DoubleBigDecimalOrder extends HetOrder[Double, BigDecimal] {
    def compare(a: Double, b: BigDecimal): Int = NumericComparisons.order(a, b)
  }
}

final class ArrayCPathComparator[@spec(Boolean, Long, Double, AnyRef) A, @spec(Boolean, Long, Double, AnyRef) B](
    lPath: CPath, lCol: HomogeneousArrayColumn[_], rPath: CPath, rCol: HomogeneousArrayColumn[_])(implicit
    ma: Manifest[A], mb: Manifest[B], ho: HetOrder[A, B]) extends CPathComparator {

  private def makeMask(path: CPath): Array[Boolean] = {
    val length = path.nodes.foldLeft(0) {
      case (len, CPathIndex(_) | CPathArray) => len + 1
      case (len, _) => len
    }

    val mask = new Array[Boolean](length)
    path.nodes.zipWithIndex foreach {
      case (CPathArray, i) => mask(i) = true
      case _ =>
    }
  }

  private val lMask: Array[Boolean] = makeMask(lPath)
  private val rMask: Array[Boolean] = makeMask(rPath)

  val aSelector = new ArraySelector[A]
  val bSelector = new ArraySelector[B]

  def compare(r1: Int, r2: Int, indices: Array[Int]): MaybeOrdering = {
    val left = lCol(r1)
    val right = rCol(r2)

    val lPluckable = selector.canPluck(left, indices, lMask)
    val rPluckable = selector.canPluck(right, indices, rMask)

    if (lPluckable) {
      if (rPluckable) {
        val a = aSelector.pluck(left, indices, lMask)
        val b = bSelector.pluck(right, indices, rMask)
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
final class ArraySelector[@specialized(Boolean, Long, Double) A](implicit m: Manifest[A]) {
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

    return true
  }

  def pluck(a: Array[_], indices: Array[Int], mask: Array[Boolean]): A = {
    var arr: Array[_] = a
    var i = 0
    var foundResult = false
    var result: A = _

    while (i < mask.length && !foundResult) {
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

