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

import com.precog.yggdrasil.table._
import com.precog.util._

import org.joda.time.DateTime

import java.nio.{ ByteBuffer, CharBuffer }
import java.nio.charset.{ Charset, CharsetEncoder, CoderResult }

import scala.collection.immutable.BitSet
import scala.collection.mutable

import scala.annotation.tailrec
import scala.{ specialized => spec }

import java.math.MathContext


/**
 * Codecs allow a writer to deal with the case where we have a buffer overflow
 * when attempting to write all data to the buffer. This lets the writer return
 * some state indicating more data needs to be written. This state is then
 * given to a `writeMore` method so it can finish the writing. It may take
 * several calls to `writeMore` before it all is finally written.
 */
trait Codec[@spec(Boolean, Long, Double) A] { self =>
  type S

  /** Returns the exact encoded size of `a`. */
  def encodedSize(a: A): Int

  /** Returns an upper bound on the size of `a`. */
  def boundSize(a: A): Int = encodedSize(a)

  def writeInit(a: A, buffer: ByteBuffer): Option[S]
  def writeMore(s: S, buffer: ByteBuffer): Option[S]

  def writeUnsafe(a: A, buffer: ByteBuffer): Unit

  def read(buffer: ByteBuffer): A

  def as[B](to: B => A, from: A => B): Codec[B] = new Codec[B] {
    type S = self.S

    def encodedSize(b: B) = self.encodedSize(to(b))
    override def boundSize(b: B) = self.boundSize(to(b))

    def writeUnsafe(b: B, buf: ByteBuffer) = self.writeUnsafe(to(b), buf)
    def writeInit(b: B, buf: ByteBuffer) = self.writeInit(to(b), buf)
    def writeMore(s: S, buf: ByteBuffer) = self.writeMore(s, buf)

    def read(src: ByteBuffer): B = from(self.read(src))
  }
}



object Codec {

  @inline def apply[A](implicit codec: Codec[A]): Codec[A] = codec

  private val byteBufferPool = new ByteBufferPool()

  def writeToArray[A](a: A)(implicit codec: Codec[A]): Array[Byte] = {
    @tailrec def loop(s: Option[codec.S], buffers: List[ByteBuffer]): Array[Byte] = s match {
      case Some(s) =>
        val buf = byteBufferPool.acquire
        loop(codec.writeMore(s, buf), buf :: buffers)

      case None =>
        buffers foreach { _.flip() }
        val out = Array.ofDim[Byte](buffers.foldLeft(0)(_ + _.remaining))
        buffers.foldRight(0) { (buffer, offset) =>
          val len = buffer.remaining
          buffer.get(out, offset, len)
          byteBufferPool.release(buffer)
          offset + len
        }
        out
    }

    val initBuffer = byteBufferPool.acquire
    loop(codec.writeInit(a, initBuffer), initBuffer :: Nil)
  }

  private final val FALSE_VALUE = 0.toByte
  private final val TRUE_VALUE = 1.toByte

  def forCType(cType: CType): Codec[_] = cType match {
    case cType: CValueType[_] => forCValueType(cType)
    case _: CNullType => ConstCodec(true)
  }

  def forCValueType[A](cType: CValueType[A]): Codec[A] = cType match {
    case CBoolean => BooleanCodec
    case CString => Utf8Codec
    case CLong => LongCodec
    case CDouble => DoubleCodec
    case CNum => BigDecimalCodec
    case CDate => DateCodec
    case CArrayType(elemType) => IndexedSeqCodec(Codec.forCValueType(elemType))
  }


  trait FixedWidthCodec[@spec(Boolean, Long, Double) A] extends Codec[A] {
    type S = A

    def size: Int

    def encodedSize(a: A) = size

    def writeInit(a: A, b: ByteBuffer): Option[A] = if (b.remaining >= size) {
      writeUnsafe(a, b)
      None
    } else {
      Some(a)
    }

    def writeMore(a: A, b: ByteBuffer): Option[A] = writeInit(a, b)
  }


  case class ConstCodec[A](a: A) extends FixedWidthCodec[A] {
    val size = 0
    def writeUnsafe(a: A, sink: ByteBuffer): Unit = ()
    def read(buffer: ByteBuffer): A = a
  }


  implicit case object BooleanCodec extends FixedWidthCodec[Boolean] {
    val size = 1
    def writeUnsafe(x: Boolean, sink: ByteBuffer) {
      if (x) sink.put(TRUE_VALUE) else sink.put(FALSE_VALUE)
    }
    def read(src: ByteBuffer): Boolean = src.get() match {
      case TRUE_VALUE => true
      case FALSE_VALUE => false
      case invalid => sys.error("Error reading boolean: expecting %d or %d, found %d" format (TRUE_VALUE, FALSE_VALUE, invalid))
    }
  }


  implicit case object LongCodec extends FixedWidthCodec[Long] {
    val size = 8
    def writeUnsafe(n: Long, sink: ByteBuffer) { sink.putLong(n) }
    def read(src: ByteBuffer): Long = src.getLong()
  }


  implicit val DateCodec = LongCodec.as[DateTime](_.getMillis, new DateTime(_))


  implicit case object DoubleCodec extends FixedWidthCodec[Double] {
    val size = 8
    def writeUnsafe(n: Double, sink: ByteBuffer) { sink.putDouble(n) }
    def read(src: ByteBuffer): Double = src.getDouble()
  }


  // TODO I guess UTF-8 is always available?
  val Utf8Charset = Charset.forName("UTF-8")

  implicit case object Utf8Codec extends Codec[String] {

    type S = Either[String, (CharBuffer, CharsetEncoder)]

    override def boundSize(s: String): Int = s.length * 4

    def encodedSize(s: String): Int = {
      var i = 0
      var size = 0
      while (i < s.length) {
        val ch = s.codePointAt(i)
        if (ch < 0x80) {
          size += 1
        } else if (ch < 0x800) {
          size += 2
        } else if (ch < 0x10000) {
          size += 3
        } else {
          size += 4
          i += 1
        }
        i += 1
      }
      size
    }

    def writeUnsafe(a: String, sink: ByteBuffer) {
      val bytes = a.getBytes(Utf8Charset)
      sink.putInt(bytes.length)
      sink.put(a.getBytes(Utf8Charset))
    }

    def writeInit(a: String, sink: ByteBuffer): Option[S] = {
      if (sink.remaining < 4) Some(Left(a)) else {
        sink.putInt(encodedSize(a))

        val source = CharBuffer.wrap(a)
        val encoder = Utf8Charset.newEncoder

        if (encoder.encode(source, sink, true) == CoderResult.OVERFLOW) {
          Some(Right((source, encoder)))
        } else {
          None
        }
      }
    }

    def writeMore(more: S, sink: ByteBuffer): Option[S] = more match {
      case Left(a) => writeInit(a, sink)
      case Right((source, encoder)) =>
        if ((encoder.encode(source, sink, true) == CoderResult.OVERFLOW) ||
            (encoder.flush(sink) == CoderResult.OVERFLOW)) {
          Some(Right((source, encoder)))
        } else {
          None
        }
    }

    def read(src: ByteBuffer): String = {
      val bytes = new Array[Byte](src.getInt())
      src.get(bytes)
      new String(bytes, Utf8Charset)
    }
  }


  // TODO Create a proper codec for BigDecimals.
  implicit val BigDecimalCodec = Utf8Codec.as[BigDecimal](_.toString, BigDecimal(_, MathContext.UNLIMITED))


  final class IndexedSeqCodec[A](val elemCodec: Codec[A]) extends Codec[IndexedSeq[A]] {

    type S = Either[IndexedSeq[A], (elemCodec.S, List[A])]

    def encodedSize(as: IndexedSeq[A]): Int = as.foldLeft(0) { (acc, a) =>
      acc + elemCodec.encodedSize(a)
    } + 4

    def writeUnsafe(as: IndexedSeq[A], sink: ByteBuffer) {
      sink.putInt(as.length)
      as foreach { elemCodec.writeUnsafe(_, sink) }
    }

    @tailrec
    private def writeArray(as: List[A], sink: ByteBuffer): Option[S] = as match {
      case a :: as => elemCodec.writeInit(a, sink) match {
        case Some(s) => Some(Right((s, as)))
        case None => writeArray(as, sink)
      }
      case _ => None
    }

    def writeInit(as: IndexedSeq[A], sink: ByteBuffer): Option[S] = {
      if (sink.remaining < 4) Some(Left(as)) else {
        sink.putInt(as.length)
        writeArray(as.toList, sink)
      }
    }

    def writeMore(more: S, sink: ByteBuffer): Option[S] = more match {
      case Left(as) => writeInit(as, sink)
      case Right((s, as)) => elemCodec.writeMore(s, sink) map (Right(_, as)) orElse writeArray(as.toList, sink)
    }

    def read(src: ByteBuffer): IndexedSeq[A] =
      ((0 until src.getInt()) map (_ => elemCodec.read(src))).toIndexedSeq
  }

  implicit def IndexedSeqCodec[A](implicit elemCodec: Codec[A]) = new IndexedSeqCodec(elemCodec)

  implicit def ArrayCodec[A: Codec: Manifest] = Codec[IndexedSeq[A]].as[Array[A]](_.toIndexedSeq, _.toArray)


  /** A Codec that can (un)wrap CValues of type CValueType. */
  case class CValueCodec[A](cType: CValueType[A])(implicit val codec: Codec[A])
      extends Codec[CWrappedValue[A]] {
    type S = codec.S
    def encodedSize(a: CWrappedValue[A]) = codec.encodedSize(a.value)
    def writeUnsafe(a: CWrappedValue[A], sink: ByteBuffer) = codec.writeUnsafe(a.value, sink)
    def writeInit(a: CWrappedValue[A], sink: ByteBuffer) = codec.writeInit(a.value, sink)
    def writeMore(s: S, sink: ByteBuffer) = codec.writeMore(s, sink)
    def read(src: ByteBuffer) = cType(codec.read(src))
  }


  // Problem: This can't be specialised.
  trait StatefulCodec {
    type A
    val codec: Codec[A]

    def init(a: A, sink: ByteBuffer): Option[State] =
      codec.writeInit(a, sink) map (State(_))

    case class State(s: codec.S) {
      def more(sink: ByteBuffer): Option[State] = codec.writeMore(s, sink) map (State(_))
    }
  }

  def wrappedWriteInit[AA](a: AA, sink: ByteBuffer)(implicit _codec: Codec[AA]): Option[StatefulCodec#State] = (new StatefulCodec {
    type A = AA
    val codec = _codec
  }).init(a, sink)


  case class RowCodec(cTypes: List[CType]) extends Codec[List[CValue]] {
    implicit val bitSetCodec: Codec[BitSet] = BitSetCodec // SparseBitSetCodec(cTypes.size)
    private val codecs: List[Codec[_ <: CValue]] = cTypes map {
      case cType: CValueType[_] => CValueCodec(cType)(forCValueType(cType))
      case cType: CNullType => ConstCodec(cType)
    }

    type S = (Either[bitSetCodec.S, StatefulCodec#State], List[CValue])

    private def undefineds(xs: List[CValue]): BitSet = BitSet(xs.zipWithIndex collect {
      case (CUndefined, i) => i
    }: _*)

    def encodedSize(xs: List[CValue]) = xs.foldLeft(bitSetCodec.encodedSize(undefineds(xs))) {
      (acc, x) => acc + (x match {
        case x: CWrappedValue[_] => forCValueType(x.cType).encodedSize(x.value)
        case _ => 0
      })
    }

    override def boundSize(xs: List[CValue]) = xs.foldLeft(bitSetCodec.boundSize(undefineds(xs))) {
      (acc, x) => acc + (x match {
        case x: CWrappedValue[_] => forCValueType(x.cType).boundSize(x.value)
        case _ => 0
      })
    }

    def writeUnsafe(xs: List[CValue], sink: ByteBuffer) {
      bitSetCodec.writeUnsafe(undefineds(xs), sink)
      xs foreach {
        case x: CWrappedValue[_] => forCValueType(x.cType).writeUnsafe(x.value, sink)
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
        case CArray(x, cType) => wrappedWriteInit(x, sink)(forCValueType(cType))
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

    def readIntoColumns(src: ByteBuffer, row: Int, colCodecs: Seq[ColCodec[_]]) {
      val undefined = bitSetCodec.read(src)
      for {
        (codec, i) <- colCodecs.zipWithIndex if !undefined(i)
      } yield codec.decode(row, src)
    }

    private def writeFromColumn(row: Int, colCodec: ColCodec[_], buffers: List[ByteBuffer]): List[ByteBuffer] = {

      @tailrec def loop(s: Option[colCodec.codec.S], buffers: List[ByteBuffer]): List[ByteBuffer] = s match {
        case Some(s) =>
          val buf = Codec.byteBufferPool.acquire
          loop(colCodec.codec.writeMore(s, buf), buf :: buffers)

        case None => buffers
      }

      val (buf, bufs) = buffers match {
        case buf :: bufs => (buf, bufs)
        case _ => (Codec.byteBufferPool.acquire, Nil)
      }
      loop(colCodec.writeInit(row, buf), buf :: bufs)
    }

    def readFromColumns(row: Int, colCodecs: List[ColCodec[_]]): Array[Byte] = {
      val undefined = BitSet(colCodecs.zipWithIndex collect {
        case (codec, i) if codec.column isDefinedAt row => i
      }: _*)

      val bitSetCodec = Codec[BitSet]

      @tailrec
      def writeBitSet(s: Option[bitSetCodec.S], buffers: List[ByteBuffer]): List[ByteBuffer] = s match {
        case Some(s) =>
          val buf = Codec.byteBufferPool.acquire
          writeBitSet(bitSetCodec.writeMore(s, buf), buf :: buffers)
        case None => buffers
      }

      val buf = Codec.byteBufferPool.acquire
      val initBuffers = writeBitSet(bitSetCodec.writeInit(undefined, buf), buf :: Nil)
      val buffers = (colCodecs.zipWithIndex filter (c => undefined(c._2))).foldLeft(initBuffers) {
        case (buffers, (colCodec, _)) => writeFromColumn(row, colCodec, buffers)
      }

      buffers foreach (_.flip())

      val out = Array.ofDim[Byte](buffers.foldLeft(0)(_ + _.remaining))
      buffers.foldRight(0) { (buffer, offset) =>
        val len = buffer.remaining
        buffer.get(out, offset, len)
        Codec.byteBufferPool.release(buffer)
        offset + len
      }

      out
    }
  }


  private def bitSet2Array(bs: BitSet): Array[Long] = {
    val size = if (bs.isEmpty) 0 else { (bs.max >>> 6) + 1 }
    val bytes = Array.ofDim[Long](size)
    bs foreach { i =>
      bytes(i >>> 6) |= 1L << (i & 0x3F)
    }
    bytes
  }

  implicit val BitSetCodec = Codec[Array[Long]].as[BitSet](bitSet2Array(_), BitSet.fromArray(_))

  case class SparseBitSetCodec(size: Int) extends Codec[BitSet] {

    // The maxBytes is max. bits / 8 = (highestOneBit(size) << 2) / 8
    private val maxBytes = java.lang.Integer.highestOneBit(size) >>> 1

    type S = (Array[Byte], Int)

    def encodedSize(bs: BitSet) = writeBitSet(bs).size
    override def boundSize(bs: BitSet) = maxBytes

    def writeUnsafe(bs: BitSet, sink: ByteBuffer) {
      sink.put(writeBitSet(bs))
    }

    def writeInit(bs: BitSet, sink: ByteBuffer): Option[S] = {
      val spaceLeft = sink.remaining()
      val bytes = writeBitSet(bs)

      if (spaceLeft >= bytes.length) {
        sink.put(bytes)
        None
      } else {
        sink.put(bytes, 0, spaceLeft)
        Some((bytes, spaceLeft))
      }
    }

    def writeMore(more: S, sink: ByteBuffer): Option[S] = {
      val (bytes, offset) = more
      val bytesLeft = bytes.length - offset
      val spaceLeft = sink.remaining()

      if (spaceLeft >= bytesLeft) {
        sink.put(bytes, offset, bytesLeft)
        None
      } else {
        sink.put(bytes, offset, spaceLeft)
        Some((bytes, offset + spaceLeft))
      }
    }

    def read(src: ByteBuffer): BitSet = readBitSet(src)


    def writeBitSet(bs: BitSet): Array[Byte] = {
      val bytes = Array.ofDim[Byte](maxBytes)

      def set(offset: Int) {
        val i = offset >>> 3
        bytes(i) = ((1 << (offset & 0x7)) | bytes(i).toInt).toByte
      }

      def rec(bs: List[Int], l: Int, r: Int, offset: Int): Int = {
        val c = (l + r) / 2

        if (c == l) {
          offset
        } else {
          bs partition (_ < c) match {
            case (Nil, Nil) =>
              offset
            case (Nil, hi) =>
              set(offset + 1)
              rec(hi, c, r, offset + 2)
            case (lo, Nil) =>
              set(offset)
              rec(lo, l, c, offset + 2)
            case (lo, hi) =>
              set(offset)
              set(offset + 1)
              rec(hi, c, r, rec(lo, l, c, offset + 2))
          }
        }
      }

      val len = rec(bs.toList, 0, size, 0)
      java.util.Arrays.copyOf(bytes, (len >>> 3) + 1) // The +1 covers the extra 2 '0' bits.
    }

    def readBitSet(src: ByteBuffer): BitSet = {
      @tailrec @inline
      def readBytes(bs: List[Byte]): Array[Byte] = {
        val b = src.get()
        if ((b & 3) == 0 || (b & 12) == 0 || (b & 48) == 0 || (b & 192) == 0) {
          (b :: bs).reverse.toArray
        } else readBytes(b :: bs)
      }

      val bytes = readBytes(Nil)
      @inline def get(offset: Int): Boolean = (bytes(offset >>> 3) & (1 << (offset & 7))) != 0

      import collection.mutable
      var bits = mutable.BitSet()
      def read(l: Int, r: Int, offset: Int): Int = {
        if (l == r) {
          offset
        } else if (r - l == 1) {
          bits(l) = true
          offset
        } else {
          val c = (l + r) / 2
          (get(offset), get(offset + 1)) match {
            case (false, false) => offset + 2
            case (false, true) => read(c, r, offset + 2)
            case (true, false) => read(l, c, offset + 2)
            case (true, true) => read(c, r, read(l, c, offset + 2))
          }
        }
      }

      read(0, size, 0)
      bits.toImmutable
    }
  }
}


// case class RowCodec(codecs: Seq[Either[CNullType, Codec.CValueCodec[_]]]) {
//   import Codec.CValueCodec
// 
//   def encode(values: Seq[CValue], sink: ByteBuffer) = {
//     assert(codecs.size == values.size)
//     (codecs zip values) foreach {
//       case (_, CUndefined) =>
// 
//       case (Left(CNull), CNull) =>
//       case (Left(CEmptyArray), CEmptyArray) =>
//       case (Left(CEmptyObject), CEmptyObject) =>
//       case (Left(CUndefined), _) => sys.error("This doesn't even make sense.")
// 
//       case (Right(codec0), v) =>
//         val codec: CValueCodec[_] = codec0
// 
//         (codec, v) match {
//           case (codec @ CValueCodec(CString), CString(s)) =>
//             codec.codec.writeUnsafe(s, sink)
//           case (codec @ CValueCodec(CBoolean), CBoolean(x)) =>
//             codec.codec.writeUnsafe(x, sink)
//           case (codec @ CValueCodec(CLong), CLong(x)) =>
//             codec.codec.writeUnsafe(x, sink)
//           case (codec @ CValueCodec(CDouble), CDouble(x)) =>
//             codec.codec.writeUnsafe(x, sink)
//           case (codec @ CValueCodec(CNum), CNum(x)) =>
//             codec.codec.writeUnsafe(x, sink)
//           case (codec @ CValueCodec(CDate), CDate(x)) =>
//             codec.codec.writeUnsafe(x, sink)
//           case (codec @ CValueCodec(codecCType: CArrayType[a]), CArray(xs, valueCType)) if codecCType == valueCType =>
//             // TODO Get rid of the cast.
//             codec.codec.writeUnsafe(xs.asInstanceOf[IndexedSeq[a]], sink)
//           case _ =>
//             sys.error("Cannot write value of type %s in column of type %s." format (v.cType, codec.cType))
//         }
//     }
//   }
// }

// trait SerializationFormat {
//   implicit def LongCodec: Codec[Long]
//   implicit def DoubleCodec: Codec[Long]
//   implicit def BigDecimal: Codec[BigDecimal]
//   implicit def DateTime: Codec[DateTime]
//   implicit def Boolean: Codec[Boolean]
//   implicit def String: Codec[String]
//   implicit def BitSetCodec: Codec[BitSet]
//
//   def writeColumns(colCodec: ColCodec[_])
// }

trait ColCodec[@spec(Boolean, Long, Double) A] {
  val codec: Codec[A]
  val column: ArrayColumn[A]

  def writeInit(row: Int, src: ByteBuffer): Option[codec.S]
  def decode(row: Int, src: ByteBuffer): Unit // This method is intentionally left blank (@spec problem).
}


object ColCodec {

  def constant(_column: ArrayColumn[Boolean]) = new ColCodec[Boolean] {
    val codec = Codec.ConstCodec(true)
    val column = _column
    def writeInit(row: Int, src: ByteBuffer) = None
    def decode(row: Int, data: ByteBuffer) = column.update(row, codec.read(data))
  }

  def forCType(cType: CType, sliceSize: Int): ColCodec[_] = cType match {
    case cType: CValueType[_] => forCValueType(cType, sliceSize)
    case CNull => constant(MutableNullColumn.empty())
    case CEmptyObject => constant(MutableEmptyObjectColumn.empty())
    case CEmptyArray => constant(MutableEmptyArrayColumn.empty())
    case CUndefined => sys.error("CUndefined cannot be serialized")
  }

  def forCValueType[A](cType: CValueType[A], sliceSize: Int): ColCodec[A] = cType match {
    case CBoolean => new ColCodec[Boolean] {
      val codec = Codec[Boolean]
      val column = ArrayBoolColumn.empty()
      def writeInit(row: Int, src: ByteBuffer) = codec.writeInit(column(row), src)
      def decode(row: Int, data: ByteBuffer) = column.update(row, codec.read(data))
    }
    case CLong => new ColCodec[Long] {
      val codec = Codec[Long]
      val column = ArrayLongColumn.empty(sliceSize)
      def writeInit(row: Int, src: ByteBuffer) = codec.writeInit(column(row), src)
      def decode(row: Int, data: ByteBuffer) = column.update(row, codec.read(data))
    }
    case CDouble => new ColCodec[Double] {
      val codec = Codec[Double]
      val column = ArrayDoubleColumn.empty(sliceSize)
      def writeInit(row: Int, src: ByteBuffer) = codec.writeInit(column(row), src)
      def decode(row: Int, data: ByteBuffer) = column.update(row, codec.read(data))
    }
    case CNum => new ColCodec[BigDecimal] {
      val codec = Codec[BigDecimal]
      val column = ArrayNumColumn.empty(sliceSize)
      def writeInit(row: Int, src: ByteBuffer) = codec.writeInit(column(row), src)
      def decode(row: Int, data: ByteBuffer) = column.update(row, codec.read(data))
    }
    case CString => new ColCodec[String] {
      val codec = Codec[String]
      val column = ArrayStrColumn.empty(sliceSize)
      def writeInit(row: Int, src: ByteBuffer) = codec.writeInit(column(row), src)
      def decode(row: Int, data: ByteBuffer) = column.update(row, codec.read(data))
    }
    case CDate => new ColCodec[DateTime] {
      val codec = Codec[DateTime]
      val column = ArrayDateColumn.empty(sliceSize)
      def writeInit(row: Int, src: ByteBuffer) = codec.writeInit(column(row), src)
      def decode(row: Int, data: ByteBuffer) = column.update(row, codec.read(data))
    }
    case cType: CArrayType[a] => new ColCodec[IndexedSeq[a]] {
      val codec = Codec.forCValueType(cType)
      val column = ArrayHomogeneousArrayColumn.empty(sliceSize)(cType.elemType)
      def writeInit(row: Int, src: ByteBuffer) = codec.writeInit(column(row), src)
      def decode(row: Int, data: ByteBuffer) = column.update(row, codec.read(data))
    }
  }
}


