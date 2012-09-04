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

import com.precog.util.ByteBufferPool

import org.joda.time.DateTime

import java.nio.ByteBuffer

import scala.collection.immutable.BitSet

import org.specs2._
import org.specs2.mutable.Specification
import org.scalacheck.{Shrink, Arbitrary, Gen}

class CodecSpec extends Specification with ScalaCheck {
  import Arbitrary._
  import ByteBufferPool._

  implicit lazy val arbBigDecimal: Arbitrary[BigDecimal] = Arbitrary(
    Gen.chooseNum(Double.MinValue / 2, Double.MaxValue / 2) map (BigDecimal(_)))

  implicit def arbBitSet = Arbitrary(Gen.listOf(Gen.choose(0, 500)) map (BitSet(_: _*)))

  implicit def arbSparseBitSet: Arbitrary[(Codec[BitSet], BitSet)] = {
    Arbitrary(Gen.chooseNum(0, 500) flatMap { size =>
      val codec = Codec.SparseBitSetCodec(size)
      if (size > 0) {
        Gen.listOf(Gen.choose(0, size - 1)) map { bits =>
          (codec, BitSet(bits: _*))
        }
      } else {
        Gen.value((codec, BitSet()))
      }
    })
  }

  implicit def arbIndexedSeq[A](implicit a: Arbitrary[A]): Arbitrary[IndexedSeq[A]] =
    Arbitrary(Gen.listOf(a.arbitrary) map (Vector(_: _*)))

  val pool = new ByteBufferPool()
  val smallPool = new ByteBufferPool(capacity = 10)

  def surviveEasyRoundTrip[A](a: A)(implicit codec: Codec[A]) = {
    val buf = pool.acquire
    codec.writeUnsafe(a, buf)
    buf.flip()
    codec.read(buf) must_== a
  }

  def surviveHardRoundTrip[A](a: A)(implicit codec: Codec[A]) = {
    val bytes = smallPool.run(for {
      _ <- codec.write(a)
      bytes <- flipBytes
      _ <- release
    } yield bytes)
    bytes.length must_== codec.encodedSize(a)
    codec.read(ByteBuffer.wrap(bytes)) must_== a
  }

  "constant codec" should {
    "write 0 bytes" in {
      val codec = Codec.ConstCodec(true)
      codec.encodedSize(true) must_== 0
      codec.read(ByteBuffer.wrap(new Array[Byte](0))) must_== true
      codec.writeUnsafe(true, ByteBuffer.allocate(0))
    }
  }

  def surviveRoundTrip[A](codec: Codec[A])(implicit a: Arbitrary[A], s: Shrink[A]) = "survive round-trip" in {
    "with large buffers" in { check { (a: A) => surviveEasyRoundTrip(a)(codec) } }
    "with small buffers" in { check { (a: A) => surviveHardRoundTrip(a)(codec) } }
  }

  "LongCodec" should surviveRoundTrip(Codec.LongCodec)
  "PackedLongCodec" should surviveRoundTrip(Codec.LongCodec)
  "BooleanCodec" should surviveRoundTrip(Codec.BooleanCodec)
  "DoubleCodec" should surviveRoundTrip(Codec.DoubleCodec)
  "Utf8Codec" should surviveRoundTrip(Codec.Utf8Codec)
  "BigDecimalCodec" should surviveRoundTrip(Codec.BigDecimalCodec)(arbBigDecimal, implicitly)
  "BitSetCodec" should surviveRoundTrip(Codec.BitSetCodec)
  "SparseBitSet" should {
    "survive round-trip" in {
      "with large buffers" in {
        check { (sparse: (Codec[BitSet], BitSet)) =>
          surviveEasyRoundTrip(sparse._2)(sparse._1)
        }
      }
      "with small buffers" in {
        check { (sparse: (Codec[BitSet], BitSet)) =>
          surviveHardRoundTrip(sparse._2)(sparse._1)
        }
      }
    }
  }
  "IndexedSeqCodec" should {
    "survive round-trip" in {
      "with large buffers" in {
        check { (xs: IndexedSeq[Long]) => surviveEasyRoundTrip(xs) }
        check { (xs: IndexedSeq[IndexedSeq[Long]]) => surviveEasyRoundTrip(xs) }
        check { (xs: IndexedSeq[String]) => surviveEasyRoundTrip(xs) }
      }
      "with small buffers" in {
        check { (xs: IndexedSeq[Long]) => surviveHardRoundTrip(xs) }
        check { (xs: IndexedSeq[IndexedSeq[Long]]) => surviveHardRoundTrip(xs) }
        check { (xs: IndexedSeq[String]) => surviveHardRoundTrip(xs) }
      }
    }
  }
  // "CValueCodec" should surviveRoundTrip(Codec.CValueCodec)
}

