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

import blueeyes.json.JPath
import org.joda.time.DateTime

import java.nio.{ ByteBuffer, CharBuffer }
import java.nio.charset.{ Charset, CharsetEncoder, CoderResult }

import scala.collection.mutable.ArrayBuffer

import scala.annotation.tailrec
import scala.{ specialized => spec }

import java.math.MathContext

object ColumnCodec {
  val readOnly = new ColumnCodec(0) // For read only work, the buffer is always provided
}


/**
 * Codecs allow a writer to deal with the case where we have a buffer overflow
 * when attempting to write all data to the buffer. This lets the writer return
 * some state indicating more data needs to be written. This state is then
 * given to a `writeMore` method so it can finish the writing. It may take
 * several calls to `writeMore` before it all is finally written.
 */
trait Codec[@spec(Boolean, Long, Double) A] { self =>
  type S

  def encodedSize(a: A): Int

  def writeInit(a: A, buffer: ByteBuffer): Option[S]
  def writeMore(s: S, buffer: ByteBuffer): Option[S]

  def writeUnsafe(a: A, buffer: ByteBuffer): Unit

  def read(buffer: ByteBuffer): A

  def as[B](to: B => A, from: A => B): Codec[B] = new Codec[B] {
    type S = self.S

    def encodedSize(b: B) = self.encodedSize(to(b))

    def writeUnsafe(b: B, buf: ByteBuffer) = self.writeUnsafe(to(b), buf)
    def writeInit(b: B, buf: ByteBuffer) = self.writeInit(to(b), buf)
    def writeMore(s: S, buf: ByteBuffer) = self.writeMore(s, buf)

    def read(src: ByteBuffer): B = from(self.read(src))
  }
}

// StringCodec = Codec[String, (CharBuffer, CharsetEncoder)]

object Codec {
  import CTypeMappings._
 
  private final val FALSE_VALUE = 0.toByte
  private final val TRUE_VALUE = 1.toByte

  // TODO I guess UTF-8 is always available?
  val Utf8Charset = Charset.forName("UTF-8")

  /*
  def forCType(cType: CType): Codec[_] = cType match {
    case CBoolean => BooleanCodec
    case CString => Utf8Codec
    case CLong => LongCodec
    case CDouble => DoubleCodec
    case CNum => BigDecimalCodec
    case CArrayType(elemType) => ArrayCodec(Codec.forCType(elemType))
    case CNull =>
    case _ => sys.error("todo")
  }
  */


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


  case class SingletonCodec[A](a: A) extends FixedWidthCodec[A] {
    val size = 0
    def writeUnsafe(a: A, sink: ByteBuffer) { }
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


  implicit case object DoubleCodec extends FixedWidthCodec[Double] {
    val size = 8
    def writeUnsafe(n: Double, sink: ByteBuffer) { sink.putDouble(n) }
    def read(src: ByteBuffer): Double = src.getDouble()
  }


  implicit case object Utf8Codec extends Codec[String] {

    type S = (CharBuffer, CharsetEncoder)

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
      }
      size
    }

    def writeUnsafe(a: String, sink: ByteBuffer) {
      sink.put(a.getBytes(Utf8Charset))
    }

    def writeInit(a: String, sink: ByteBuffer): Option[S] = {
      val source = CharBuffer.wrap(a)
      // TODO How much work is it to create a newEncoder? Why is this not
      // immutable? Ugh....
      val encoder = Utf8Charset.newEncoder

      if (encoder.encode(source, sink, true) == CoderResult.OVERFLOW) {
        Some((source, encoder))
      } else {
        None
      }
    }

    def writeMore(more: S, sink: ByteBuffer): Option[S] = {
      val (source, encoder) = more

      if ((encoder.encode(source, sink, true) == CoderResult.OVERFLOW) ||
          (encoder.flush(sink) == CoderResult.OVERFLOW)) {
        Some((source, encoder))
      } else {
        None
      }
    }

    def read(src: ByteBuffer): String = sys.error("todo")
  }


  // TODO Create a proper codec for BigDecimals.
  implicit val BigDecimalCodec = Utf8Codec.as[BigDecimal](_.toString, BigDecimal(_, MathContext.UNLIMITED))


  final class ArrayCodec[A](val elemCodec: Codec[A]) extends Codec[IndexedSeq[A]] {

    type S = Either[IndexedSeq[A], (elemCodec.S, Stream[A])]

    def encodedSize(as: IndexedSeq[A]): Int = as.foldLeft(0) { (acc, a) =>
      acc + elemCodec.encodedSize(a)
    } + 4

    def writeUnsafe(as: IndexedSeq[A], sink: ByteBuffer) {
      sink.putInt(as.length)
      as foreach { elemCodec.writeUnsafe(_, sink) }
    }

    @tailrec
    private def writeArray(as: Stream[A], sink: ByteBuffer): Option[S] = as match {
      case a #:: as => elemCodec.writeInit(a, sink) match {
        case Some(s) => Some(Right((s, as)))
        case None => writeArray(as, sink)
      }
      case _ => None
    }

    def writeInit(as: IndexedSeq[A], sink: ByteBuffer): Option[S] = {
      if (sink.remaining < 4) Some(Left(as)) else {
        sink.putInt(as.length)
        writeArray(as.toStream, sink)
      }
    }

    def writeMore(more: S, sink: ByteBuffer): Option[S] = more match {
      case Left(as) => writeInit(as, sink)
      case Right((s, as)) => elemCodec.writeMore(s, sink) map (Right(_, as)) orElse writeArray(as.toStream, sink)
    }

    def read(src: ByteBuffer): IndexedSeq[A] =
      ((0 until src.getInt()) map (_ => elemCodec.read(src))).toIndexedSeq
  }

  implicit def ArrayCodec[A](implicit elemCodec: Codec[A]) = new ArrayCodec(elemCodec)
}


/**
 * This class is responsible for encoding and decoding a Seq[(ColumnRef,Column)]
 * into a byte array for serialization. It is *not* thread-safe.
 */
class ColumnCodec(bufferSize: Int = (16 * 1024)) {
  import CTypeMappings._

  private final val workBuffer = ByteBuffer.allocate(bufferSize)
  private final val DefaultCharset = "UTF-8"
  
  private final val FALSE_VALUE = 0.toByte
  private final val TRUE_VALUE = 1.toByte

  def write[A](cType: CValueType[A], a: A): Unit = cType match {
    case CString => writeString(a)
    case CBoolean => writeBoolean(a)
    case CLong => workBuffer.putLong(a)
    case CDouble => workBuffer.putDouble(a)
    case CNum => writeBigDecimal(a)
    case CDate => workBuffer.putLong(a.getMillis)
    case CArrayType(elemType) =>
      workBuffer.putInt(a.length)
      a foreach (write(elemType, _))
  }

  // This generates a NoSuchMethod error for array types, for some reason.
  //private def writerFor[A](cType: CValueType[A]): (A => Unit) = cType match {
    //case CString => writeString(_)
    //case CBoolean => writeBoolean(_)
    //case CLong => workBuffer.putLong(_)
    //case CDouble => workBuffer.putDouble(_)
    //case CNum => writeBigDecimal(_)
    //case CDate => dt => workBuffer.putLong(dt.getMillis)
    //case CArrayType(elemType) => writeArray(_, writerFor(elemType))
  //}

  private def writeArray[A](s: IndexedSeq[A], w: A => Unit) {
    workBuffer.putInt(s.length)
    s foreach { a =>
      w(a)
    }
  }

  private def writeString(s: String) {
    // RLE Strings
    val bytes = s.getBytes(DefaultCharset)
    workBuffer.putInt(bytes.length)
    workBuffer.put(bytes)
  }

  private def writeBigDecimal(bd: BigDecimal) {
    writeString(bd.toString) // TODO: Figure out a sane way to serialize BigDecimal
  }

  private def writeBoolean(b: Boolean) {
    workBuffer.put(if (b) TRUE_VALUE else FALSE_VALUE)
  }

  private def readerFor[A](cType: CValueType[A]): ByteBuffer => A = cType match {
    case CString => readString(_)
    case CBoolean => readBoolean(_)
    case CLong => _.getLong()
    case CDouble => _.getDouble()
    case CNum => readBigDecimal(_)
    case CDate => buffer => new DateTime(buffer.getLong())
    case CArrayType(elemType) =>
      readArray(_, readerFor(elemType))
  }

  private def readArray[A](buffer: ByteBuffer, readElem: ByteBuffer => A) = {
    val length = buffer.getInt()
    ((0 until length) map (_ => readElem(buffer))).toIndexedSeq
  }

  private def readString(buffer: ByteBuffer): String = {
    // TODO: Could possibly be more efficient here with allocations
    val bytes = new Array[Byte](buffer.getInt())
    buffer.get(bytes)
    new String(bytes, DefaultCharset)
  }

  private def readBigDecimal(buffer: ByteBuffer): BigDecimal = {
    BigDecimal(readString(buffer), java.math.MathContext.UNLIMITED)
  }

  private def readBoolean(buffer: ByteBuffer): Boolean = {
    buffer.get() match {
      case TRUE_VALUE  => true
      case FALSE_VALUE => false
      case invalid     => sys.error("Invalid boolean encoded value: " + invalid)
    }
  }

  @tailrec
  private def writeFlagFor(cType: CType) {
    workBuffer.put(flagFor(cType))
    cType match {
      case CArrayType(elemType) =>
        writeFlagFor(elemType)
      case _ =>
    }
  }

  def encode(values: Seq[CValue]): Array[Byte] = {
    workBuffer.clear()

    values.foreach { value => {
      if (value == CUndefined) {
        workBuffer.put(FUNDEFINED)
      } else {
        writeFlagFor(value.cType)
        value match {
          case CString(cs)  => writeString(cs)                  
          case CBoolean(cb) => writeBoolean(cb)
          case CLong(cl)    => workBuffer.putLong(cl)
          case CDouble(cd)  => workBuffer.putDouble(cd)
          case CNum(cn)     => writeBigDecimal(cn)
          case CDate(cd)    => workBuffer.putLong(cd.getMillis)
          case CArray(as, cType) => write(cType, as)
          case CNull        => // NOOP, no value to write
          case CEmptyObject => // NOOP, no value to write
          case CEmptyArray  => // NOOP, no value to write
          case CUndefined   => // NOOP, no value to write
        }
      }
    }}

    // Read bytes out of the bytebuffer into a new array
    val outBytes = new Array[Byte](workBuffer.position())
    workBuffer.flip()
    workBuffer.get(outBytes)

    outBytes
  }

  def encode(columns: Seq[(ColumnRef, Column)], row: Int, encodeRef: Boolean = false): Array[Byte] = {
    workBuffer.clear()

    columns.foreach {
      case (ref, column) => {
        if (encodeRef) {
          writeString(ref.selector.toString)
        }
        
        if (column.isDefinedAt(row)) {
          workBuffer.put(flagFor(ref.ctype))
          ref.ctype match {
            case CString      => writeString(column.asInstanceOf[StrColumn].apply(row))                  
            case CBoolean     => writeBoolean(column.asInstanceOf[BoolColumn].apply(row))                
            case CLong        => workBuffer.putLong(column.asInstanceOf[LongColumn].apply(row))          
            case CDouble      => workBuffer.putDouble(column.asInstanceOf[DoubleColumn].apply(row))      
            case CNum         => writeBigDecimal(column.asInstanceOf[NumColumn].apply(row))              
            case CDate        => workBuffer.putLong(column.asInstanceOf[DateColumn].apply(row).getMillis)
            case CArrayType(elemType) => sys.error("TODO: cannot encode array columns yet")
            case CNull        => // No value encoded
            case CEmptyObject => // No value encoded
            case CEmptyArray  => // No value encoded
            case CUndefined   => sys.error("Cannot encode an undefined column")
          }
        } else {
          workBuffer.put(FUNDEFINED)
        }
      }
    }

    // Read bytes out of the bytebuffer into a new array
    val outBytes = new Array[Byte](workBuffer.position())
    workBuffer.flip()
    workBuffer.get(outBytes)

    outBytes
  }

  private def readCType(buffer: ByteBuffer): CType = buffer.get() match {
    case FSTRING => CString
    case FBOOLEAN => CBoolean
    case FLONG => CLong
    case FDOUBLE => CDouble
    case FNUM => CNum
    case FDATE => CDate
    case FARRAY =>
      readCType(buffer) match {
        case elemType: CValueType[_] => CArrayType(elemType)
        case badType => sys.error("Invalid array element type: " + badType)
      }
    case FNULL => CNull
    case FEMPTYOBJECT => CEmptyObject
    case FEMPTYARRAY => CEmptyArray
    case FUNDEFINED => CUndefined
    case invalid => sys.error("Invalid format flag: " + invalid)
  }

  private def readToCValue(buffer: ByteBuffer): CValue = {
    readCType(buffer) match {
      case cType: CValueType[_] => cType(readerFor(cType)(buffer))
      case cType: CNullType => cType
    }
  }

  def decodeWithRefs(input: Array[Byte]): Array[(String, CValue)] = decodeWithRefs(ByteBuffer.wrap(input))

  def decodeWithRefs(buffer: ByteBuffer): Array[(String, CValue)] = {
    var resultBuffer = ArrayBuffer[(String, CValue)]()

    while (buffer.hasRemaining()) {
      val selector = readString(buffer)
      resultBuffer.append((selector, readToCValue(buffer)))
    }

    resultBuffer.toArray
  }

  def decodeToCValues(input: Array[Byte]): Array[CValue] = decodeToCValues(ByteBuffer.wrap(input))

  def decodeToCValues(buffer: ByteBuffer): Array[CValue] = {
    var resultBuffer = ArrayBuffer[CValue]()

    while (buffer.hasRemaining()) {
      resultBuffer.append(readToCValue(buffer))
    }

    resultBuffer.toArray
  }

  def decodeToArrayColumns(input: Array[Byte], row: Int, columns: Array[ArrayColumn[_]]) {
    decodeToArrayColumns(ByteBuffer.wrap(input), row, columns)
  }

  /**
   * Decode the given byte buffer, storing its values in the proper ArrayColumns
   * @param buffer The buffer to read from. Must be ready for reads (position == 0)
   */
  def decodeToArrayColumns(buffer: ByteBuffer, row: Int, columns: Array[ArrayColumn[_]]) {
    var columnIndex = 0

    while (buffer.hasRemaining()) {
      readCType(buffer) match {
        case CString       => columns(columnIndex).asInstanceOf[ArrayStrColumn].update(row, readString(buffer))
        case CBoolean      => columns(columnIndex).asInstanceOf[ArrayBoolColumn].update(row, readBoolean(buffer))
        case CLong         => columns(columnIndex).asInstanceOf[ArrayLongColumn].update(row, buffer.getLong())
        case CDouble       => columns(columnIndex).asInstanceOf[ArrayDoubleColumn].update(row, buffer.getDouble())
        case CNum          => columns(columnIndex).asInstanceOf[ArrayNumColumn].update(row, readBigDecimal(buffer))
        case CDate         => columns(columnIndex).asInstanceOf[ArrayDateColumn].update(row, new DateTime(buffer.getLong()))
        case cType @ CArrayType(_) =>
          val col = columns(columnIndex).asInstanceOf[ArrayHomogeneousArrayColumn[AnyRef]]
          col.update(row, readerFor(cType)(buffer).asInstanceOf[IndexedSeq[AnyRef]])
        case CNull         => columns(columnIndex).asInstanceOf[MutableNullColumn].update(row, true)
        case CEmptyObject  => columns(columnIndex).asInstanceOf[MutableEmptyObjectColumn].update(row, true)
        case CEmptyArray   => columns(columnIndex).asInstanceOf[MutableEmptyArrayColumn].update(row, true)          
        case CUndefined    => // NOOP, array/mutable columns start fully undefined                                  
        case invalid       => sys.error("Invalid format flag: " + invalid)                                          
      }                                                                                                             
      columnIndex += 1                                                                                              
    }                                                                                                               
  }                                                                                                                 
}                                                                                                                   
                                                                                                                    
                                                                                                                    
