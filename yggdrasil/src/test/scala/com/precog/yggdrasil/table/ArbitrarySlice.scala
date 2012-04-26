package com.precog.yggdrasil

import com.precog.common._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary._

trait ArbitrarySlice extends ArbitraryProjectionDescriptor {
  def genColumn(col: ColumnDescriptor, size: Int): Gen[Array[_]] = {
    col.valueType match {
      case CStringArbitrary => containerOfN[Array, String](size, arbitrary[String])
      case CStringFixed(w) =>  containerOfN[Array, String](size, arbitrary[String].filter(_.length < w))
      case CBoolean => containerOfN[Array, Boolean](size, arbitrary[Boolean])
      case CInt => containerOfN[Array, Int](size, arbitrary[Int])
      case CLong => containerOfN[Array, Long](size, arbitrary[Long])
      case CFloat => containerOfN[Array, Float](size, arbitrary[Float])
      case CDouble => containerOfN[Array, Double](size, arbitrary[Double])
      case CDecimalArbitrary => containerOfN[List, Double](size, arbitrary[Double]).map(_.map(v => BigDecimal(v)).toArray)
    }
  }

  def genSlice(p: ProjectionDescriptor, size: Int): Gen[Slice] = {
    def sequence[T](l: List[Gen[T]], acc: Gen[List[T]]): Gen[List[T]] = {
      l match {
        case x :: xs => acc.flatMap(l => sequence(xs, x.map(xv => xv :: l)))
        case Nil => acc
      }
    }

    for {
      ids <- listOfN(p.identities, listOfN(size, arbitrary[Long]).map(_.sorted.toArray))
      data <- sequence(p.columns.map(cd => genColumn(cd, size).map(col => (cd, col))), value(Nil))
    } yield {
      val dataMap = data map {
        case (ColumnDescriptor(path, selector, ctype, _), arr) => (VColumnRef[ctype.CA](NamedColumnId(path, selector), ctype) -> arr)
      }

      new ArraySlice(VectorCase(ids: _*), dataMap.toMap)
    }
  }
}
