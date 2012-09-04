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
package jdbm3

import scala.collection.immutable.BitSet

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
  implicit def BitSetCodec: Codec[BitSet] = Codec.BitSetCodec
  // @transient implicit lazy val BitSetCodec: Codec[BitSet] = Codec.SparseBitSetCodec(columnRefs.size)
  implicit def IndexedSeqCodec[A](implicit elemCodec: Codec[A]): Codec[IndexedSeq[A]] = Codec.IndexedSeqCodec(elemCodec)
}


