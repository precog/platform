package com.precog.yggdrasil
package jdbm3

//import scala.collection.immutable.BitSet
import com.precog.util.BitSet
import com.precog.util.BitSetUtil
import com.precog.util.BitSetUtil.Implicits._

import org.joda.time.DateTime


/**
 * Defines a base set of codecs that are often used in `RowFormat`s.
 */
trait StdCodecs {
  implicit def LongCodec: Codec[Long]
  implicit def DoubleCodec: Codec[Double]
  implicit def BigDecimalCodec: Codec[BigDecimal]
  implicit def StringCodec: Codec[String]
  implicit def BooleanCodec: Codec[Boolean]
  implicit def DateTimeCodec: Codec[DateTime]
  implicit def BitSetCodec: Codec[BitSet]
  implicit def IndexedSeqCodec[A](implicit elemCodec: Codec[A]): Codec[IndexedSeq[A]]

  def codecForCType(cType: CType): Codec[_] = cType match {
    case cType: CValueType[_] => codecForCValueType(cType)
    case _: CNullType => Codec.ConstCodec(true)
  }

  def codecForCValueType[A](cType: CValueType[A]): Codec[A] = try { cType match {
    case CBoolean => BooleanCodec
    case CString => StringCodec
    case CLong => LongCodec
    case CDouble => DoubleCodec
    case CNum => BigDecimalCodec
    case CDate => DateTimeCodec
  } } catch {
    case ex: Throwable =>
      println(cType)
      throw ex
    }
}


trait RowFormatCodecs extends StdCodecs { self: RowFormat =>
  implicit def LongCodec: Codec[Long] = Codec.PackedLongCodec
  implicit def DoubleCodec: Codec[Double] = Codec.DoubleCodec
  implicit def BigDecimalCodec: Codec[BigDecimal] = Codec.BigDecimalCodec
  implicit def StringCodec: Codec[String] = Codec.Utf8Codec
  implicit def BooleanCodec: Codec[Boolean] = Codec.BooleanCodec
  implicit def DateTimeCodec: Codec[DateTime] = Codec.DateCodec
  // implicit def BitSetCodec: Codec[BitSet] = Codec.BitSetCodec
  //@transient implicit lazy val BitSetCodec: Codec[BitSet] = Codec.SparseBitSetCodec(columnRefs.size)
  @transient implicit lazy val BitSetCodec: Codec[BitSet] = Codec.SparseBitSetCodec(columnRefs.size)
  implicit def IndexedSeqCodec[A](implicit elemCodec: Codec[A]): Codec[IndexedSeq[A]] = Codec.IndexedSeqCodec(elemCodec)
}


