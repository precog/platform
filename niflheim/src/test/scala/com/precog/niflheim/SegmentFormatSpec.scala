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

import com.precog.util._
import com.precog.util.BitSetUtil.Implicits._

import org.joda.time.DateTime

import org.specs2._
import org.specs2.mutable.Specification
import org.scalacheck._

import scalaz._

class V1SegmentFormatSpec extends SegmentFormatSpec {
  val format = V1SegmentFormat
}

class VersionedSegmentFormatSpec extends Specification with ScalaCheck with SegmentFormatSupport with SegmentFormatMatchers {
  val format = VersionedSegmentFormat(Map(
    1 -> V1SegmentFormat,
    2 -> new StubSegmentFormat // Much faster version of segment formats.
  ))

  "versioned segment formats" should {
    "read older versions" in {
      implicit val arbSegment = Arbitrary(genSegment(100))
      val old = VersionedSegmentFormat(Map(1 -> V1SegmentFormat))

      check { (segment0: Segment) =>
        val out = new InMemoryWritableByteChannel
        old.writer.writeSegment(out, segment0) must beLike { case Success(_) =>
          val in = new InMemoryReadableByteChannel(out.toArray)
          format.reader.readSegment(in) must beLike { case Success(segment1) =>
            areEqual(segment0, segment1)
          }
        }
      }
    }
  }
}

trait SegmentFormatSpec extends Specification with ScalaCheck with SegmentFormatSupport with SegmentFormatMatchers {
  def format: SegmentFormat

  def surviveRoundTrip(segment: Segment) = surviveRoundTripWithFormat(format)(segment)

  val EmptyBitSet = BitSetUtil.create()

  override val defaultPrettyParams = Pretty.Params(2)

  "segment formats" should {
    "roundtrip trivial null segments" in {
      surviveRoundTrip(NullSegment(1234L, CPath("a.b.c"), CNull, EmptyBitSet, 0))
      surviveRoundTrip(NullSegment(1234L, CPath("a.b.c"), CEmptyObject, EmptyBitSet, 0))
      surviveRoundTrip(NullSegment(1234L, CPath("a.b.c"), CEmptyArray, EmptyBitSet, 0))
    }
    "roundtrip trivial boolean segments" in surviveRoundTrip(BooleanSegment(1234L, CPath("a.b.c"), EmptyBitSet, EmptyBitSet, 0))
    "roundtrip trivial array segments" in {
      surviveRoundTrip(ArraySegment(1234L, CPath("a.b.c"), CLong, EmptyBitSet, new Array[Long](0)))
      surviveRoundTrip(ArraySegment(1234L, CPath("a.b.c"), CDouble, EmptyBitSet, new Array[Double](0)))
      surviveRoundTrip(ArraySegment(1234L, CPath("a.b.c"), CNum, EmptyBitSet, new Array[BigDecimal](0)))
      surviveRoundTrip(ArraySegment(1234L, CPath("a.b.c"), CString, EmptyBitSet, new Array[String](0)))
      surviveRoundTrip(ArraySegment(1234L, CPath("a.b.c"), CDate, EmptyBitSet, new Array[DateTime](0)))
    }
    "roundtrip simple boolean segment" in {
      val segment = BooleanSegment(1234L, CPath("a.b.c"),
        BitSetUtil.create(Seq(0)), BitSetUtil.create(Seq(0)), 1)
      surviveRoundTrip(segment)
    }
    "roundtrip undefined boolean segment" in {
      val segment = BooleanSegment(1234L, CPath("a.b.c"),
        EmptyBitSet, EmptyBitSet, 10)
      surviveRoundTrip(segment)
    }
    "roundtrip simple array segment" in {
      val segment = ArraySegment(1234L, CPath("a.b.c"), CDouble,
        BitSetUtil.create(Seq(0)), Array(4.2))
      surviveRoundTrip(segment)
    }
    "roundtrip undefined array segment" in {
      val segment = ArraySegment(1234L, CPath("a.b.c"), CDouble,
        EmptyBitSet, new Array[Double](100))
      surviveRoundTrip(segment)
    }
    "roundtrip arbitrary small segments" in {
      implicit val arbSegment = Arbitrary(genSegment(100))
      check { (segment: Segment) =>
        surviveRoundTrip(segment)
      }
    }
    "roundtrip arbitrary large segments" in {
      implicit val arbSegment = Arbitrary(genSegment(10000))
      check { (segment: Segment) =>
        surviveRoundTrip(segment)
      }
    }
  }
}

