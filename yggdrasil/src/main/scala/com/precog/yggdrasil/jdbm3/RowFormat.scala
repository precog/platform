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

import com.precog.common.json._
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

  def compare(a: Array[Byte], b: Array[Byte]): Int = {
    val selectors = columnRefs map (_.selector)
    val aVals = selectors zip decode(a) groupBy (_._1)
    val bVals = selectors zip decode(b) groupBy (_._1)

    val cmp = selectors.distinct.iterator map { cPath =>
      val a = aVals(cPath) find (_._2 != CUndefined)
      val b = bVals(cPath) find (_._2 != CUndefined)
      (a, b) match {
        case (None, None) => 0
        case (None, _) => -1
        case (_, None) => 1
        case (Some((_, a)), Some((_, b))) => CValue.compareValues(a, b)
      }
    } find (_ != 0) getOrElse 0

    cmp
  }
}


object RowFormat {
  def forSortingKey(columnRefs: Seq[ColumnRef]): RowFormat = SortingKeyRowFormatV1(columnRefs)

  def forValues(columnRefs: Seq[ColumnRef]): RowFormat = ValueRowFormatV1(columnRefs)


  case class ValueRowFormatV1(_columnRefs: Seq[ColumnRef]) extends ValueRowFormat with RowFormatCodecs {

    // This is really stupid, but required to work w/ JDBM.
    @transient lazy val columnRefs: Seq[ColumnRef] = _columnRefs map { ref =>
      ref.copy(ctype = ref.ctype.readResolve())
    }

    // TODO Get this from somewhere else?
    @transient lazy val pool = new ByteBufferPool()
  }

  case class SortingKeyRowFormatV1(_columnRefs: Seq[ColumnRef]) extends RowFormatCodecs with SortingRowFormat {
    @transient lazy val columnRefs: Seq[ColumnRef] = _columnRefs map { ref =>
      ref.copy(ctype = ref.ctype.readResolve())
    }

    @transient lazy val pool = new ByteBufferPool()
  }
}


trait RowFormatSupport { self: StdCodecs =>
  import ByteBufferPool._

  def getColumnEncoder(cType: CType, col: Column): Int => ByteBufferPoolS[Unit] = (cType, col) match {
    case (CLong, col: LongColumn) =>
      (row: Int) => Codec[Long].write(col(row))
    case (CDouble, col: DoubleColumn) =>
      (row: Int) => Codec[Double].write(col(row))
    case (CNum, col: NumColumn) =>
      (row: Int) => Codec[BigDecimal].write(col(row))
    case (CBoolean, col: BoolColumn) =>
      (row: Int) => Codec[Boolean].write(col(row))
    case (CString, col: StrColumn) =>
      (row: Int) => Codec[String].write(col(row))
    case (CDate, col: DateColumn) =>
      (row: Int) => Codec[DateTime].write(col(row))
    case (CEmptyObject, col: EmptyObjectColumn) =>
      (row: Int) => Codec.ConstCodec(true).write(true)
    case (CEmptyArray, col: EmptyArrayColumn) =>
      (row: Int) => Codec.ConstCodec(true).write(true)
    case (CNull, col: NullColumn) =>
      (row: Int) => Codec.ConstCodec(true).write(true)
    case (cType, col) => sys.error(
      "Cannot create column encoder, columns of wrong type (expected %s, found %s)." format (cType, col.tpe))
  }


  def getColumnDecoder(cType: CType, col: ArrayColumn[_]): (Int, ByteBuffer) => Unit = (cType, col) match {
    case (CLong, col: ArrayLongColumn) =>
      (row: Int, buf: ByteBuffer) => col.update(row, Codec[Long].read(buf))
    case (CDouble, col: ArrayDoubleColumn) =>
      (row: Int, buf: ByteBuffer) => col.update(row, Codec[Double].read(buf))
    case (CNum, col: ArrayNumColumn) =>
      (row: Int, buf: ByteBuffer) => col.update(row, Codec[BigDecimal].read(buf))
    case (CBoolean, col: ArrayBoolColumn) =>
      (row: Int, buf: ByteBuffer) => col.update(row, Codec[Boolean].read(buf))
    case (CString, col: ArrayStrColumn) =>
      (row: Int, buf: ByteBuffer) => col.update(row, Codec[String].read(buf))
    case (CDate, col: ArrayDateColumn) =>
      (row: Int, buf: ByteBuffer) => col.update(row, Codec[DateTime].read(buf))
    case (CEmptyObject, col: MutableEmptyObjectColumn) =>
      (row: Int, buf: ByteBuffer) => col.update(row, true)
    case (CEmptyArray, col: MutableEmptyArrayColumn) =>
      (row: Int, buf: ByteBuffer) => col.update(row, true)
    case (CNull, col: MutableNullColumn) =>
      (row: Int, buf: ByteBuffer) => col.update(row, true)
    case _ => sys.error("Cannot create column decoder, columns of wrong type.")
  }
}


trait ValueRowFormat extends RowFormat with RowFormatSupport { self: StdCodecs =>
  import ByteBufferPool._

  def pool: ByteBufferPool

  def encode(cValues: List[CValue]) = getBytesFrom(RowCodec.writeAll(cValues)(pool.acquire _).reverse)

  def decode(bytes: Array[Byte], offset: Int): List[CValue] =
    RowCodec.read(ByteBuffer.wrap(bytes, offset, bytes.length - offset))

  def ColumnEncoder(cols: Seq[Column]) = {
    require(columnRefs.size == cols.size)

    val colWriters: Seq[(Int => ByteBufferPoolS[Unit], Int)] =
      (columnRefs zip cols map { case (ColumnRef(_, cType), col) => getColumnEncoder(cType, col) }).zipWithIndex

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
      (columnRefs zip cols map { case (ref, col) => getColumnDecoder(ref.ctype, col) }).zipWithIndex

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


/**
 * This is a row format that is optimized for quickly comparing 2 encoded rows
 * (ie. byte arrays).
 */
trait SortingRowFormat extends RowFormat with StdCodecs with RowFormatSupport {
  import SortingRowFormat._

  def pool: ByteBufferPool

  override implicit def StringCodec = Codec.Utf8Codec

  @transient
  abstract override implicit lazy val BigDecimalCodec: Codec[BigDecimal] =
    Codec.CompositeCodec[Double, BigDecimal, BigDecimal](Codec[Double], super.BigDecimalCodec, bd => (bd.toDouble, bd), (_, bd) => bd)

  @transient lazy val selectors: List[(CPath, List[CType])] = {
    val refs: Map[CPath, Seq[ColumnRef]] = columnRefs.groupBy(_.selector)
    (columnRefs map (_.selector)).distinct.map(selector => (selector, refs(selector).map(_.ctype).toList))(collection.breakOut)
  }

  private def zipWithSelectors[A](xs: Seq[A]): List[(CPath, Seq[(A, CType)])] = {
    @tailrec
    def zip(zipped: List[(CPath, Seq[(A, CType)])], right: Seq[A], sels: List[(CPath, List[CType])]): List[(CPath, Seq[(A, CType)])] = sels match {
      case Nil => zipped.reverse
      case (path, cTypes) :: sels =>
        val (head, tail) = right splitAt cTypes.size
        zip((path, head zip cTypes) :: zipped, tail, sels)
    }

    zip(Nil, xs, selectors)
  }

  def ColumnEncoder(cols: Seq[Column]): ColumnEncoder = {
    import ByteBufferPool._

    val colWriters: List[Int => ByteBufferPoolS[Unit]] = zipWithSelectors(cols) map { case (_, colsAndTypes) =>
      val writers: Seq[Int => ByteBufferPoolS[Unit]] = colsAndTypes map {
        case (col, cType) =>
          val writer = getColumnEncoder(cType, col)
          (row: Int) => writeFlagFor(cType) flatMap (_ => writer(row))
      }

      val selCols: Seq[Column] = colsAndTypes map (_._1)

      (row: Int) => (writers zip selCols) find (_._2.isDefinedAt(row)) map (_._1(row)) getOrElse writeFlagFor(CUndefined)
    }

    new ColumnEncoder {
      import scalaz.syntax.apply._
      import scalaz.syntax.monad._

      def encodeFromRow(row: Int) = {
        val rowWriter = colWriters.foldLeft(().point[ByteBufferPoolS]) {
          case (acc, encode) => acc *> encode(row)
        }

        pool.run(for {
          _ <- rowWriter
          bytes <- flipBytes
          _ <- release
        } yield bytes)
      }
    }
  }


  def ColumnDecoder(cols: Seq[ArrayColumn[_]]): ColumnDecoder = {
    val decoders: List[Map[Byte, (Int, ByteBuffer) => Unit]] =
      zipWithSelectors(cols) map { case (_, colsWithTypes) =>
        val decoders: Map[Byte, (Int, ByteBuffer) => Unit] =
          (for ((col, cType) <- colsWithTypes) yield {
            (flagForCType(cType), getColumnDecoder(cType, col))
          })(collection.breakOut)

        decoders
      }

    new ColumnDecoder {
      def decodeToRow(row: Int, src: Array[Byte], offset: Int = 0) {
        val buf = ByteBuffer.wrap(src, offset, src.length - offset)

        @tailrec
        def decode(decoders: List[Map[Byte, (Int, ByteBuffer) => Unit]]): Unit = decoders match {
          case selDecoder :: decoders =>
            val flag = buf.get()
            if (flag != FUndefined) {
              selDecoder(flag)(row, buf)
            }
            decode(decoders)
          case Nil =>
            // Do nothing.
        }

        decode(decoders)
      }
    }
  }

  def encode(cValues: List[CValue]): Array[Byte] = {

    val cvals: List[CValue] = zipWithSelectors(cValues) map {
      case (_, cvals) => cvals map (_._1) find (_ != CUndefined) getOrElse CUndefined
    }

    import ByteBufferPool._

    import scalaz.syntax.traverse._
    import scalaz.std.list._

    val writes: ByteBufferPoolS[List[Unit]] = cvals.map {
      case v: CNullValue =>
        writeFlagFor(v.cType)

      case v: CWrappedValue[_] =>
        for {
          _ <-writeFlagFor(v.cType)
          _ <- codecForCValueType(v.cType).write(v.value)
        } yield ()
    }.sequence


    pool.run(for {
      _ <- writes
      bytes <- flipBytes
      _ <- release
    } yield bytes)
  }

  def decode(bytes: Array[Byte], offset: Int = 0): List[CValue] = {
    val buf = ByteBuffer.wrap(bytes)

    def readForSelector(cTypes: List[CType]): List[CValue] = {
      val cValue = cTypeForFlag(buf.get()) match {
        case cType: CValueType[_] =>
          cType(codecForCValueType(cType).read(buf))
        case cType: CNullType =>
          cType
      }

      val cType = cValue.cType
      cTypes map {
        case `cType` => cValue
        case _ => CUndefined
      }
    }

    selectors.map { case (_, cTypes) =>
      readForSelector(cTypes)
    }.flatten
  }

  override def compare(a: Array[Byte], b: Array[Byte]): Int = {
    val abuf = ByteBuffer.wrap(a)
    val bbuf = ByteBuffer.wrap(b)

    @inline
    def compareNext(): Int = {
      val aType = abuf.get()
      val bType = bbuf.get()

      if ((aType & 0xF0) == (bType & 0xF0)) {
        ((aType & 0xF0).toByte) match {
          case FUndefined => 0
          case FBoolean =>
            abuf.get() - bbuf.get()
          case FString =>
            Codec.Utf8Codec.compare(abuf, bbuf)
          case FNumeric =>
            aType match {
              case FLong =>
                val a = Codec[Long].read(abuf)
                bType match {
                  case FLong =>
                    NumericComparisons.compare(a, Codec[Long].read(bbuf))
                  case FDouble =>
                    NumericComparisons.compare(a, Codec[Double].read(bbuf))
                  case FBigDecimal =>
                    val b = Codec[Double].read(bbuf)
                    NumericComparisons.approxCompare(a.toDouble, b) match {
                      case 0 =>
                        BigDecimal(a) compare super.BigDecimalCodec.read(bbuf)
                      case cmp =>
                        super.BigDecimalCodec.skip(bbuf)
                        cmp
                    }
                }
              case FDouble =>
                val a = Codec[Double].read(abuf)
                bType match {
                  case FLong =>
                    NumericComparisons.compare(a, Codec[Long].read(bbuf))
                  case FDouble =>
                    NumericComparisons.compare(a, Codec[Double].read(bbuf))
                  case FBigDecimal =>
                    val b = Codec[Double].read(bbuf)
                    NumericComparisons.approxCompare(a, b) match {
                      case 0 =>
                        BigDecimal(a) compare super.BigDecimalCodec.read(bbuf)
                      case cmp =>
                        super.BigDecimalCodec.skip(bbuf)
                        cmp
                    }

                }
              case FBigDecimal =>
                val a = Codec[Double].read(abuf)
                bType match {
                  case FLong =>
                    val b = Codec[Long].read(bbuf)
                    NumericComparisons.approxCompare(a, b.toDouble) match {
                      case 0 =>
                        super.BigDecimalCodec.read(abuf) compare BigDecimal(b)
                      case cmp =>
                        super.BigDecimalCodec.skip(abuf)
                        cmp
                    }
                  case FDouble =>
                    val b = Codec[Double].read(bbuf)
                    NumericComparisons.approxCompare(a, b) match {
                      case 0 =>
                        super.BigDecimalCodec.read(abuf) compare BigDecimal(b)
                      case cmp =>
                        super.BigDecimalCodec.skip(abuf)
                        cmp
                    }
                  case FBigDecimal =>
                    val b = Codec[Double].read(bbuf)
                    NumericComparisons.approxCompare(a, b) match {
                      case 0 =>
                        super.BigDecimalCodec.read(abuf) compare super.BigDecimalCodec.read(bbuf)
                      case cmp =>
                        super.BigDecimalCodec.skip(abuf)
                        super.BigDecimalCodec.skip(bbuf)
                        cmp
                    }
                }
            }
          case FEmptyObject => 0
          case FEmptyArray => 0
          case FNull => 0
          case FDate =>
            math.signum(Codec[Long].read(abuf) - Codec[Long].read(bbuf)).toInt
          case x => sys.error("Match error for: " + x)
        }
      } else {
        (aType.toInt & 0xFF) - (bType.toInt & 0xFF)
      }
    }

    @tailrec
    def compare(cmp: Int): Int = if (cmp == 0) {
      if (abuf.remaining() > 0) compare(compareNext()) else 0
    } else cmp

    compare(0)
  }
}

object SortingRowFormat {
  def writeFlagFor[M[+_]](cType: CType)(implicit M: ByteBufferMonad[M]): M[Unit] = {
    import scalaz.syntax.monad._

    val flag = flagForCType(cType)
    for (buf <- M.getBuffer(1)) yield {
      buf.put(flag)
      ()
    }
  }

  def flagForCType(cType: CType): Byte = cType match {
    case CBoolean => FBoolean
    case CString => FString
    case CLong => FLong
    case CDouble => FDouble
    case CNum => FBigDecimal
    case CDate => FDate
    case CEmptyObject => FEmptyObject
    case CEmptyArray => FEmptyArray
    case CNull => FNull
    case CUndefined => FUndefined
  }

  def cTypeForFlag(flag: Byte): CType = flag match {
    case FBoolean => CBoolean
    case FString => CString
    case FLong => CLong
    case FDouble => CDouble
    case FBigDecimal => CNum
    case FDate => CDate
    case FEmptyObject => CEmptyObject
    case FEmptyArray => CEmptyArray
    case FNull => CNull
    case FUndefined => CUndefined
  }

  private val FUndefined: Byte = 0x0.toByte
  private val FBoolean: Byte = 0x10.toByte
  private val FString: Byte = 0x20.toByte
  private val FNumeric: Byte = 0x40.toByte
  private val FLong: Byte = 0x41.toByte
  private val FDouble: Byte = 0x42.toByte
  private val FBigDecimal: Byte = 0x43.toByte
  private val FEmptyObject: Byte = 0x60.toByte
  private val FEmptyArray: Byte = 0x70.toByte
  private val FNull: Byte = 0x80.toByte
  private val FDate: Byte = 0x90.toByte
}
