package com.precog.niflheim

import com.precog.common._
import com.precog.common.json._

import java.io.File

import org.specs2.mutable.Specification
import org.specs2._
import org.scalacheck._

import scalaz._

class V1CookedBlockFormatSpecs extends CookedBlockFormatSpecs {
  val format = V1CookedBlockFormat
}

case class VersionedCookedBlockFormatSpecs() extends CookedBlockFormatSpecs {
  val format = VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat))
}

trait CookedBlockFormatSpecs extends Specification with ScalaCheck with SegmentFormatSupport {
  def format: CookedBlockFormat

  override val defaultPrettyParams = Pretty.Params(2)

  implicit val arbFile = Arbitrary(for {
    parts <- Gen.listOfN(3, Gen.identifier map { part =>
      part.substring(0, math.min(part.length, 5))
    })
  } yield new File(parts.mkString("/", "/", ".cooked")))

  implicit val arbSegmentId = Arbitrary(genSegmentId)

  "cooked block format" should {
    "round trip empty segments" in {
      surviveRoundTrip(format)(CookedBlockMetadata(999L, 0, new Array[(SegmentId, File)](0)))
    }

    "round trip simple segments" in {
      surviveRoundTrip(format)(CookedBlockMetadata(999L, 1, 
          Array(SegmentId(1234L, CPath("a.b.c"), CLong) -> new File("/hello/there/abc.cooked"))
      ))
    }

    "roundtrip arbitrary blocks" in {
      check { files: List[(SegmentId, File)] =>
        surviveRoundTrip(format)(CookedBlockMetadata(999L, files.length, files.toArray))
      }.set(maxDiscarded -> 2000)
    }
  }

  //def surviveRoundTrip(format: CookedBlockFormat)(segments0: Array[(SegmentId, File)]) = {
  def surviveRoundTrip(format: CookedBlockFormat)(segments0: CookedBlockMetadata) = {
    val out = new InMemoryWritableByteChannel
    format.writeCookedBlock(out, segments0) must beLike {
      case Success(_) =>
        val in = new InMemoryReadableByteChannel(out.toArray)
        format.readCookedBlock(in) must beLike {
          case Success(segments1) =>
            segments1 must_== segments0
        }
    }
  }
}
