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

import scalaz._

import com.precog.yggdrasil.table._
import com.precog.util._

import org.joda.time.DateTime

import java.nio.ByteBuffer

import scala.collection.immutable.BitSet
import scala.collection.mutable

import scala.annotation.tailrec
import scala.{ specialized => spec }


trait ColumnEncoder {
  def encodeFromRow(row: Int): Array[Byte]
}

trait ColumnDecoder {
  def decodeToRow(row: Int, src: Array[Byte], offset: Int = 0): Unit
}

trait RowFormat {

  def columnRefs: Seq[ColumnRef]

  def ColumnEncoder(cols: Seq[Column]): ColumnEncoder
  def ColumnDecoder(cols: Seq[ArrayColumn[_]]): ColumnDecoder
  
  def encode(cValues: List[CValue]): Array[Byte]
  def decode(bytes: Array[Byte], offset: Int = 0): List[CValue]
}


object RowFormat {
  def forValues(columnRefs: Seq[ColumnRef]): RowFormat = ValueRowFormatV1(columnRefs)

  case class ValueRowFormatV1(_columnRefs: Seq[ColumnRef]) extends ValueRowFormat with RowFormatCodecs {

    // This is really stupid, but required to work w/ JDBM.
    @transient lazy val columnRefs: Seq[ColumnRef] = _columnRefs map { ref =>
      ref.copy(ctype = ref.ctype.readResolve())
    }

    // TODO Get this from somewhere else?
    @transient lazy val pool = new ByteBufferPool()
  }
}


trait ValueRowFormat extends RowFormat { self: StdCodecs =>
  import ByteBufferPool._

  def pool: ByteBufferPool

  def encode(cValues: List[CValue]) = getBytesFrom(RowCodec.writeAll(cValues)(pool.acquire _).reverse)

  def decode(bytes: Array[Byte], offset: Int): List[CValue] =
    RowCodec.read(ByteBuffer.wrap(bytes, offset, bytes.length - offset))

  def ColumnEncoder(cols: Seq[Column]) = {
    require(columnRefs.size == cols.size)

    val colWriters: Seq[(Int => ByteBufferPoolS[Unit], Int)] =
      (columnRefs zip cols map (getColumnEncoder)).zipWithIndex

    new ColumnEncoder {
      import scalaz.syntax.apply._
      import scalaz.syntax.monad._

      def encodeFromRow(row: Int) = {
        val undefined = BitSet(cols.zipWithIndex collect {
          case (col, i) if !col.isDefinedAt(row) => i
        }: _*)

        val rowWriter = colWriters.foldLeft(Codec[BitSet].write(undefined)) {
          case (acc, (encode, i)) if !undefined(i) => acc *> encode(row)
          case (acc, _) => acc
        }

        pool.run(for {
          _ <- rowWriter
          bytes <- flipBytes
          _ <- release
        } yield bytes)
      }
    }
  }

  def ColumnDecoder(cols: Seq[ArrayColumn[_]]) = {
    require(columnRefs.size == cols.size)

    // TODO Function2 isn't @spec'd in 2.9.2... sigh.

    val decoders: Seq[((Int, ByteBuffer) => Unit, Int)] =
      (columnRefs zip cols map (getColumnDecoder)).zipWithIndex

    new ColumnDecoder {
      def decodeToRow(row: Int, src: Array[Byte], offset: Int = 0) {
        val buf = ByteBuffer.wrap(src, offset, src.length - offset)
        val undefined = Codec[BitSet].read(buf)
        for ((decode, i) <- decoders if !undefined(i)) {
          decode(row, buf)
        }
      }
    }
  }

  protected def getColumnEncoder(col: (ColumnRef, Column)): Int => ByteBufferPoolS[Unit] = col match {
    case (ColumnRef(_, CLong), col: LongColumn) =>
      (row: Int) => Codec[Long].write(col(row))
    case (ColumnRef(_, CDouble), col: DoubleColumn) =>
      (row: Int) => Codec[Double].write(col(row))
    case (ColumnRef(_, CNum), col: NumColumn) =>
      (row: Int) => Codec[BigDecimal].write(col(row))
    case (ColumnRef(_, CBoolean), col: BoolColumn) =>
      (row: Int) => Codec[Boolean].write(col(row))
    case (ColumnRef(_, CString), col: StrColumn) =>
      (row: Int) => Codec[String].write(col(row))
    case (ColumnRef(_, CDate), col: DateColumn) =>
      (row: Int) => Codec[DateTime].write(col(row))
    case (ColumnRef(_, CEmptyObject), col: EmptyObjectColumn) =>
      (row: Int) => Codec.ConstCodec(true).write(true)
    case (ColumnRef(_, CEmptyArray), col: EmptyArrayColumn) =>
      (row: Int) => Codec.ConstCodec(true).write(true)
    case (ColumnRef(_, CNull), col: NullColumn) =>
      (row: Int) => Codec.ConstCodec(true).write(true)
    case (ColumnRef(_, cType), col) => sys.error(
      "Cannot create column encoder, columns of wrong type (expected %s, found %s)." format (cType, col.tpe))
  }


  protected def getColumnDecoder(col: (ColumnRef, ArrayColumn[_])): (Int, ByteBuffer) => Unit = col match {
    case (ColumnRef(_, CLong), col: ArrayLongColumn) =>
      (row: Int, buf: ByteBuffer) => col.update(row, Codec[Long].read(buf))
    case (ColumnRef(_, CDouble), col: ArrayDoubleColumn) =>
      (row: Int, buf: ByteBuffer) => col.update(row, Codec[Double].read(buf))
    case (ColumnRef(_, CNum), col: ArrayNumColumn) =>
      (row: Int, buf: ByteBuffer) => col.update(row, Codec[BigDecimal].read(buf))
    case (ColumnRef(_, CBoolean), col: ArrayBoolColumn) =>
      (row: Int, buf: ByteBuffer) => col.update(row, Codec[Boolean].read(buf))
    case (ColumnRef(_, CString), col: ArrayStrColumn) =>
      (row: Int, buf: ByteBuffer) => col.update(row, Codec[String].read(buf))
    case (ColumnRef(_, CDate), col: ArrayDateColumn) =>
      (row: Int, buf: ByteBuffer) => col.update(row, Codec[DateTime].read(buf))
    case (ColumnRef(_, CEmptyObject), col: MutableEmptyObjectColumn) =>
      (row: Int, buf: ByteBuffer) => col.update(row, true)
    case (ColumnRef(_, CEmptyArray), col: MutableEmptyArrayColumn) =>
      (row: Int, buf: ByteBuffer) => col.update(row, true)
    case (ColumnRef(_, CNull), col: MutableNullColumn) =>
      (row: Int, buf: ByteBuffer) => col.update(row, true)
    case _ => sys.error("Cannot create column decoder, columns of wrong type.")
  }

  case object RowCodec extends Codec[List[CValue]] {
    import Codec.{ StatefulCodec, wrappedWriteInit }

    @transient lazy val bitSetCodec = Codec[BitSet]

    @transient private lazy val codecs: List[Codec[_ <: CValue]] = columnRefs.toList map {
      case ColumnRef(_, cType: CValueType[_]) => Codec.CValueCodec(cType)(codecForCValueType(cType))
      case ColumnRef(_, cType: CNullType) => Codec.ConstCodec(cType)
    }

    type S = (Either[bitSetCodec.S, StatefulCodec#State], List[CValue])

    private def undefineds(xs: List[CValue]): BitSet = BitSet(xs.zipWithIndex collect {
      case (CUndefined, i) => i
    }: _*)

    def encodedSize(xs: List[CValue]) = xs.foldLeft(bitSetCodec.encodedSize(undefineds(xs))) {
      (acc, x) => acc + (x match {
        case x: CWrappedValue[_] => codecForCValueType(x.cType).encodedSize(x.value)
        case _ => 0
      })
    }

    override def maxSize(xs: List[CValue]) = xs.foldLeft(bitSetCodec.maxSize(undefineds(xs))) {
      (acc, x) => acc + (x match {
        case x: CWrappedValue[_] => codecForCValueType(x.cType).maxSize(x.value)
        case _ => 0
      })
    }

    def writeUnsafe(xs: List[CValue], sink: ByteBuffer) {
      bitSetCodec.writeUnsafe(undefineds(xs), sink)
      xs foreach {
        case x: CWrappedValue[_] => codecForCValueType(x.cType).writeUnsafe(x.value, sink)
        case _ =>
      }
    }

    @tailrec
    private def writeCValues(xs: List[CValue], sink: ByteBuffer): Option[S] = xs match {
      case x :: xs => (x match {
        case CBoolean(x) => wrappedWriteInit[Boolean](x, sink)
        case CString(x) => wrappedWriteInit[String](x, sink)
        case CDate(x) => wrappedWriteInit[DateTime](x, sink)
        case CLong(x) => wrappedWriteInit[Long](x, sink)
        case CDouble(x) => wrappedWriteInit[Double](x, sink)
        case CNum(x) => wrappedWriteInit[BigDecimal](x, sink)
        case _: CNullType => None
      }) match {
        case None => writeCValues(xs, sink)
        case Some(s) => Some((Right(s), xs))
      }

      case Nil => None
    }

    def writeInit(xs: List[CValue], sink: ByteBuffer) =
      bitSetCodec.writeInit(undefineds(xs), sink) map (s => (Left(s), xs)) orElse writeCValues(xs, sink)

    def writeMore(more: S, sink: ByteBuffer) = more match {
      case (Left(s), xs) => bitSetCodec.writeMore(s, sink) map (s => (Left(s), xs)) orElse writeCValues(xs, sink)
      case (Right(s), xs) => s.more(sink) map (s => (Right(s), xs)) orElse writeCValues(xs, sink)
    }
    
    def read(src: ByteBuffer): List[CValue] = {
      val undefined = bitSetCodec.read(src)
      codecs.zipWithIndex collect {
        case (codec, i) if undefined(i) => CUndefined
        case (codec, _) => codec.read(src)
      }
    }
  }
}

