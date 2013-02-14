package com.precog.niflheim

import com.precog.common._
import com.precog.common.json._
import com.precog.util._
import com.precog.util.BitSetUtil.Implicits._

import org.joda.time.DateTime

import scala.collection.mutable

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels._

import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import org.scalacheck._

import scalaz._

trait SegmentFormatSupport {
  import Gen._
  import Arbitrary.arbitrary

  implicit lazy val arbBigDecimal: Arbitrary[BigDecimal] = Arbitrary(
    Gen.chooseNum(Double.MinValue / 2, Double.MaxValue / 2) map (BigDecimal(_)))

  def genCPath: Gen[CPath] = for {
    len <- Gen.choose(0, 5)
    parts <- Gen.listOfN(len, Gen.identifier)
  } yield CPath(parts mkString ".")

  def genBitSet(length: Int, density: Double): Gen[BitSet] = Gen { params =>
    val bits = new mutable.ArrayBuffer[Int]
    Loop.range(0, bits.length) { row =>
      if (params.rng.nextDouble < density)
        bits += row
    }
    Some(BitSetUtil.create(bits.toArray))
  }

  def genForCType[A](ctype: CValueType[A]): Gen[A] = ctype match {
    case CBoolean => arbitrary[Boolean]
    case CString => arbitrary[String]
    case CLong => arbitrary[Long]
    case CDouble => arbitrary[Double]
    case CNum => arbitrary[BigDecimal]
    case CDate => arbitrary[Long] map (new DateTime(_))
    case CArrayType(elemType: CValueType[a]) =>
      val list: Gen[List[a]] = listOf(genForCType(elemType))
      val array: Gen[Array[a]] = list map (_.toArray(elemType.manifest))
      array
  }

  def genCValueType(maxDepth: Int = 2): Gen[CValueType[_]] = {
    val basic: Gen[CValueType[_]] = oneOf(Seq(CBoolean, CString, CLong, CDouble, CNum, CDate))
    if (maxDepth > 0) {
      frequency(6 -> basic, 1 -> (genCValueType(maxDepth - 1) map (CArrayType(_))))
    } else {
      basic
    }
  }

  def genArray[A: Manifest](length: Int, g: Gen[A]): Gen[Array[A]] = for {
    values <- listOfN(length, g)
  } yield {
    val array = manifest[A].newArray(length)
    values.zipWithIndex foreach { case (v, i) =>
      array(i) = v
    }
    array
  }

  def genArraySegmentForCType[A](ctype: CValueType[A], length: Int): Gen[ArraySegment[_]] = {
    val g = genForCType(ctype)
    for {
      blockId <- arbitrary[Long]
      cpath <- genCPath
      defined <- genBitSet(length, 0.5)
      values <- genArray(length, g)(ctype.manifest) // map (toCTypeArray(ctype)) // (_.toArray(ctype.manifest))
    } yield ArraySegment(blockId, cpath, ctype, defined, values)
  }

  def genArraySegment(length: Int): Gen[ArraySegment[_]] = for {
    ctype <- genCValueType(2) filter (_ != CBoolean) // Note: CArrayType(CBoolean) is OK!
    segment <- genArraySegmentForCType(ctype, length)
  } yield segment

  def genBooleanSegment(length: Int): Gen[BooleanSegment] = for {
    blockId <- arbitrary[Long]
    cpath <- genCPath
    defined <- genBitSet(length, 0.7)
    values <- genBitSet(length, 0.5)
  } yield BooleanSegment(blockId, cpath, defined, values, length)

  def genNullSegmentForCType(ctype: CNullType, length: Int): Gen[NullSegment] = for {
    blockId <- arbitrary[Long]
    cpath <- genCPath
    defined <- genBitSet(length, 0.7)
  } yield NullSegment(blockId, cpath, ctype, defined, length)

  def genNullSegment(length: Int): Gen[NullSegment] = for {
    ctype <- oneOf(CNull, CEmptyArray, CEmptyObject)
    segment <- genNullSegmentForCType(ctype, length)
  } yield segment

  def genSegment(length: Int): Gen[Segment] =
    oneOf(genArraySegment(length), genBooleanSegment(length), genNullSegment(length))
}

trait SegmentFormatMatchers { self: Specification with ScalaCheck =>
  def areEqual(x0: Segment, y0: Segment) = {
    x0.id must_== y0.id
    x0.defined must_== y0.defined
    x0.length must_== y0.length

    val definedAt = (0 until x0.length) map x0.defined.apply
    (x0, y0) must beLike {
      case (x: ArraySegment[_], y: ArraySegment[_]) =>
        val xs = x.values.deep zip definedAt filter (_._2) map (_._1)
        val ys = y.values.deep zip definedAt filter (_._2) map (_._1)
        xs must_== ys
      case (x: BooleanSegment, y: BooleanSegment) =>
        (x.values & x0.defined) must_== (y.values & x0.defined)
      case (x: NullSegment, y: NullSegment) =>
        ok
    }
  }

  def surviveRoundTripWithFormat(format: SegmentFormat)(segment0: Segment) = {
    val out = new InMemoryWritableByteChannel
    format.writer.writeSegment(out, segment0) must beLike {
      case Success(_) =>
        format.reader.readSegment(new InMemoryReadableByteChannel(out.toArray)) must beLike {
          case Success(segment1) =>
            // 
            areEqual(segment0, segment1)
        }
    }
  }
}

final class StubSegmentFormat extends SegmentFormat {
  val TheOneSegment = NullSegment(42L, CPath("w.t.f"), CNull, BitSetUtil.create(), 100)

  object reader extends SegmentReader {
    def readSegmentId(channel: ReadableByteChannel): Validation[IOException, SegmentId] =
      Success(TheOneSegment.id)

    def readSegment(channel: ReadableByteChannel): Validation[IOException, Segment] =
      Success(TheOneSegment)
  }

  object writer extends SegmentWriter {
    def writeSegment(channel: WritableByteChannel, segment: Segment): Validation[IOException, PrecogUnit] =
      Success(PrecogUnit)
  }
}

final class InMemoryReadableByteChannel(bytes: Array[Byte]) extends ReadableByteChannel {
  val buffer = ByteBuffer.wrap(bytes)

  var isOpen = true
  def close() { isOpen = false }
  def read(dst: ByteBuffer): Int = if (buffer.remaining() == 0) {
    -1
  } else {
    val written = math.min(dst.remaining(), buffer.remaining())
    while (dst.remaining() > 0 && buffer.remaining() > 0) {
      dst.put(buffer.get())
    }
    written
  }
}

final class InMemoryWritableByteChannel extends WritableByteChannel {
  val buffer = new mutable.ArrayBuffer[Byte]

  def write(buf: ByteBuffer): Int = {
    val read = buf.remaining()
    while (buf.remaining() > 0) {
      buffer += buf.get()
    }
    read
  }

  var isOpen = true

  def close() {
    isOpen = false
  }

  def toArray: Array[Byte] = buffer.toArray
}
