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
package com.precog.niflheim

import com.precog.common._
import com.precog.common.json._

import com.precog.util.PrecogUnit
import com.precog.util.BitSet
import com.precog.util.BitSetUtil.Implicits._

import java.io.{ File, IOException }
import java.nio.channels.{ ReadableByteChannel, WritableByteChannel }
import java.nio.ByteBuffer

import scala.{ specialized => spec }
import scala.annotation.tailrec
import scala.collection.mutable

import scalaz.{ Validation, Success, Failure }

object V1SegmentFormat extends SegmentFormat {
  private val checksum = true

  private object Flags {
    val FBoolean: Byte = 1
    val FString: Byte = 2
    val FLong: Byte = 3
    val FDouble: Byte = 4
    val FBigDecimal: Byte = 5
    val FDate: Byte = 6
    val FArray: Byte = 7
    val FNull: Byte = 16
    val FEmptyArray: Byte = 17
    val FEmptyObject: Byte = 18
  }

  object reader extends SegmentReader {
    private def wrapException[A](f: => A): Validation[IOException, A] = try {
      Success(f)
    } catch { case e: Exception =>
      Failure(new IOException(e))
    }

    def readSegment(channel: ReadableByteChannel): Validation[IOException, Segment] = {
      def readHeader(): Validation[IOException, SegmentId] = for {
        buffer <- readChunk(channel)
        blockId <- wrapException(buffer.getLong())
        cpath <- wrapException(CPath(Codec.Utf8Codec.read(buffer)))
        ctype <- readCType(buffer)
      } yield SegmentId(blockId, cpath, ctype)

      def readArray[A](ctype: CValueType[A]): Validation[IOException, (BitSet, Array[A])] = for {
        buffer <- readChunk(channel)
      } yield {
        val length = buffer.getInt()
        val bitSetCodec = Codec.SparseBitSetCodec(length)
        val defined = bitSetCodec.read(buffer)
        val codec = getCodecFor(ctype)
        val values = ctype.manifest.newArray(length)
        defined.foreach { row =>
          values(row) = codec.read(buffer)
        }
        (defined, values)
      }

      def readNull(ctype: CNullType): Validation[IOException, (BitSet, Int)] = for {
        buffer <- readChunk(channel)
      } yield {
        val length = buffer.getInt()
        val bitSetCodec = Codec.SparseBitSetCodec(length)
        val defined = bitSetCodec.read(buffer)
        (defined, length)
      }

      def readBoolean(): Validation[IOException, (BitSet, Int, BitSet)] = for {
        buffer <- readChunk(channel)
      } yield {
        val length = buffer.getInt()
        val bitSetCodec = Codec.SparseBitSetCodec(length)
        val defined = bitSetCodec.read(buffer)
        val values = bitSetCodec.read(buffer)
        (defined, length, values)
      }

      for {
        header <- readHeader()
        segment <- header match {
          case SegmentId(blockid, cpath, CBoolean) =>
            readBoolean() map { case (defined, length, values) =>
              BooleanSegment(blockid, cpath, defined, values, length)
            }

          case SegmentId(blockid, cpath, ctype: CValueType[a]) =>
            readArray(ctype) map { case (defined, values) =>
              ArraySegment(blockid, cpath, ctype, defined, values)
            }

          case SegmentId(blockid, cpath, ctype: CNullType) =>
            readNull(ctype) map { case (defined, length) =>
              NullSegment(blockid, cpath, ctype, defined, length)
            }
        }
      } yield segment
    }

    def readCType(buffer: ByteBuffer): Validation[IOException, CType] = {
      import Flags._

      def readCValueType(flag: Byte): Validation[IOException, CValueType[_]] = flag match {
        case FBoolean => Success(CBoolean)
        case FString => Success(CString)
        case FLong => Success(CLong)
        case FDouble => Success(CDouble)
        case FBigDecimal => Success(CNum)
        case FDate => Success(CDate)
        case FArray => readCValueType(buffer.get()) map (CArrayType(_))
        case flag => Failure(new IOException("Unexpected segment type flag: %x" format flag))
      }

      buffer.get() match {
        case FNull => Success(CNull)
        case FEmptyArray => Success(CEmptyArray)
        case FEmptyObject => Success(CEmptyObject)
        case flag => readCValueType(flag)
      }
    }
  }

  object writer extends SegmentWriter {
    def writeSegment(channel: WritableByteChannel, segment: Segment): Validation[IOException, PrecogUnit] = {
      for {
        _ <- writeSegmentId(channel, segment)
        _ <- segment match {
          case seg: ArraySegment[a] =>
            writeArraySegment(channel, seg, getCodecFor(seg.ctype))
          case seg: BooleanSegment =>
            writeBooleanSegment(channel, seg)
          case seg: NullSegment =>
            writeNullSegment(channel, seg)
        }
      } yield PrecogUnit
    }

    private def writeSegmentId(channel: WritableByteChannel, segment: Segment): Validation[IOException, PrecogUnit] = {
      val tpeFlag = getFlagFor(segment.ctype)
      val strPath = segment.cpath.toString
      val maxSize = Codec.Utf8Codec.maxSize(strPath) + tpeFlag.length + 8

      writeChunk(channel, maxSize) { buffer =>
        buffer.putLong(segment.blockid)
        Codec.Utf8Codec.writeUnsafe(strPath, buffer)
        buffer.put(tpeFlag)
        PrecogUnit
      }
    }

    private def writeArraySegment[@spec(Boolean,Long,Double) A](channel: WritableByteChannel,
        segment: ArraySegment[A], codec: Codec[A]): Validation[IOException, PrecogUnit] = {
      val bitSetCodec = Codec.SparseBitSetCodec(segment.values.length)

      var maxSize = bitSetCodec.maxSize(segment.defined) + 4
      segment.defined.foreach { row =>
        maxSize += codec.maxSize(segment.values(row))
      }

      writeChunk(channel, maxSize) { buffer =>
        buffer.putInt(segment.values.length)
        bitSetCodec.writeUnsafe(segment.defined, buffer)
        segment.defined.foreach { row =>
          codec.writeUnsafe(segment.values(row), buffer)
        }
        PrecogUnit
      }
    }

    private def writeBooleanSegment(channel: WritableByteChannel, segment: BooleanSegment) = {
      val bitSetCodec = Codec.SparseBitSetCodec(segment.length)
      val maxSize = bitSetCodec.maxSize(segment.defined) + bitSetCodec.maxSize(segment.values) + 4
      writeChunk(channel, maxSize) { buffer =>
        buffer.putInt(segment.length)
        bitSetCodec.writeUnsafe(segment.defined, buffer)
        bitSetCodec.writeUnsafe(segment.values, buffer)
        PrecogUnit
      }
    }

    private def writeNullSegment(channel: WritableByteChannel, segment: NullSegment) = {
      val bitSetCodec = Codec.SparseBitSetCodec(segment.length)
      val maxSize = bitSetCodec.maxSize(segment.defined) + 4
      writeChunk(channel, maxSize) { buffer =>
        buffer.putInt(segment.length)
        bitSetCodec.writeUnsafe(segment.defined, buffer)
        PrecogUnit
      }
    }
  }

  private def getFlagFor(ctype: CType): Array[Byte] = {
    import Flags._

    val buffer = new mutable.ArrayBuffer[Byte]()

    def flagForCType(t: CType) {
      @tailrec
      def flagForCValueType(t: CValueType[_]) {
        t match {
          case CString => buffer += FString
          case CBoolean => buffer += FBoolean
          case CLong => buffer += FLong
          case CDouble => buffer += FDouble
          case CNum => buffer += FBigDecimal
          case CDate => buffer += FDate
          case CArrayType(tpe) =>
            buffer += FArray
            flagForCValueType(tpe)
        }
      }

      t match {
        case t: CValueType[_] =>
          flagForCValueType(t)
        case CNull =>
          buffer += FNull
        case CEmptyArray =>
          buffer += FEmptyArray
        case CEmptyObject =>
          buffer += FEmptyObject
        case CUndefined =>
          sys.error("Unexpected CUndefined type. Undefined segments don't exist!")
      }
    }

    flagForCType(ctype)
    buffer.toArray
  }

  private def allocate(size: Int): ByteBuffer = ByteBuffer.allocate(size)

  def writeChunk[A](channel: WritableByteChannel, maxSize: Int)(f: ByteBuffer => A): Validation[IOException, A] = {
    val buffer = allocate(maxSize + 4)
    buffer.position(4)

    val result = f(buffer)

    buffer.flip()
    buffer.putInt(0, buffer.limit() - 4)

    try {
      while (buffer.remaining() > 0) {
        channel.write(buffer)
      }
      Success(result)
    } catch {
      case ex: IOException =>
        Failure(ex)
    }
  }

  def readChunk(channel: ReadableByteChannel): Validation[IOException, ByteBuffer] = {
    try {
      val buffer0 = allocate(4)
      while (buffer0.remaining() > 0) {
        channel.read(buffer0)
      }
      buffer0.flip()
      val length = buffer0.getInt()

      val buffer = allocate(length)
      while (buffer.remaining() > 0) {
        channel.read(buffer)
      }
      buffer.flip()
      Success(buffer)
    } catch {
      case ioe: IOException =>
        Failure(ioe)
    }
  }

  private def getCodecFor[A](ctype: CValueType[A]): Codec[A] = ctype match {
    case CBoolean => Codec.BooleanCodec
    case CString => Codec.Utf8Codec
    case CLong => Codec.PackedLongCodec
    case CDouble => Codec.DoubleCodec
    case CNum => Codec.BigDecimalCodec
    case CDate => Codec.DateCodec
    case CArrayType(elemType) =>
      Codec.ArrayCodec(getCodecFor(elemType), elemType.manifest)
  }
}

