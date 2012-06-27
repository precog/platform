package com.precog.yggdrasil
package table

import com.precog.common._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary._
import scala.collection.BitSet

trait ArbitrarySlice extends ArbitraryProjectionDescriptor {
  def arbitraryBitSet(size: Int): Gen[BitSet] = {
    containerOfN[List, Boolean](size, arbitrary[Boolean]) map { BitsetColumn.bitset _ }
  }

  private def fullBitSet(size: Int): BitSet = {
    BitSet((0 until size).toSeq : _*)
  }

  def genColumn(col: ColumnDescriptor, size: Int): Gen[Column] = {
    val bs = fullBitSet(size)
    col.valueType match {
      case CStringArbitrary   => containerOfN[Array, String](size, arbitrary[String]) map { strs => ArrayStrColumn(bs, strs) }
      case CStringFixed(w)    => containerOfN[Array, String](size, arbitrary[String].filter(_.length < w)) map { strs => ArrayStrColumn(fullBitSet(strs.length), strs) }
      case CBoolean           => containerOfN[Array, Boolean](size, arbitrary[Boolean]) map { bools => ArrayBoolColumn(bs, bools) }
      case CLong              => containerOfN[Array, Long](size, arbitrary[Long]) map { longs => ArrayLongColumn(bs, longs) }
      case CDouble            => containerOfN[Array, Double](size, arbitrary[Double]) map { doubles => ArrayDoubleColumn(bs, doubles) }
      case CDecimalArbitrary  => containerOfN[List, Double](size, arbitrary[Double]) map { arr => ArrayNumColumn(bs, arr.map(v => BigDecimal(v)).toArray) }
      case CNull              => arbitraryBitSet(size) map { s => new BitsetColumn(s) with NullColumn }
      case CEmptyObject       => arbitraryBitSet(size) map { s => new BitsetColumn(s) with EmptyObjectColumn }
      case CEmptyArray        => arbitraryBitSet(size) map { s => new BitsetColumn(s) with EmptyArrayColumn }
    }
  }

  def genSlice(p: ProjectionDescriptor, sz: Int): Gen[Slice] = {
    def sequence[T](l: List[Gen[T]], acc: Gen[List[T]]): Gen[List[T]] = {
      l match {
        case x :: xs => acc.flatMap(l => sequence(xs, x.map(xv => xv :: l)))
        case Nil => acc
      }
    }

    for {
      ids <- listOfN(p.identities, listOfN(sz, arbitrary[Long]).map(_.sorted.toArray))
      data <- sequence(p.columns.map(cd => genColumn(cd, sz).map(col => (cd, col))), value(Nil))
    } yield {
      new Slice {
        val size = sz
        val columns: Map[ColumnRef, Column] = data.map({
          case (ColumnDescriptor(path, selector, ctype, _), arr) => (ColumnRef(selector, ctype) -> arr)
        })(collection.breakOut)
      }
    }
  }
}
