package com.precog.common

import blueeyes.json.xschema.DefaultSerialization._

import org.specs2.mutable.Specification
import org.specs2._

import org.scalacheck._

import scalaz._
import Scalaz._

class MetadataSpec extends Specification with MetadataGenerators with ScalaCheck {
  import Prop._
  
  val sampleSize = 100

  "simple metadata" should {
    "surivive round trip serialization" in check { in: Metadata =>
      in.serialize.validated[Metadata] must beLike {
        case Success(out) => in mustEqual out
      }
    }

    "merge with like metadata" in check { (sample1: List[Metadata], sample2: List[Metadata]) =>
      val prepared = sample1 zip sample2 map {
        case (e1, e2) => (e1, e2, e1 merge e2) 
      }
      
      forall(prepared) {
        case (BooleanValueStats(c1, t1), BooleanValueStats(c2, t2), Some(BooleanValueStats(c3, t3))) => {
          c3 must_== c1 + c2
          t3 must_== t1 + t2
        }
        case (LongValueStats(c1, mn1, mx1), LongValueStats(c2, mn2, mx2), Some(LongValueStats(c3, mn3, mx3))) => {
          c3 must_== c1 + c2
          mn3 must_== (mn1 min mn2)
          mx3 must_== (mx1 max mx2)
        }
        case (DoubleValueStats(c1, mn1, mx1), DoubleValueStats(c2, mn2, mx2), Some(DoubleValueStats(c3, mn3, mx3))) => {
          c3 must_== c1 + c2
          mn3 must_== (mn1 min mn2)
          mx3 must_== (mx1 max mx2)
        }
        case (BigDecimalValueStats(c1, mn1, mx1), BigDecimalValueStats(c2, mn2, mx2), Some(BigDecimalValueStats(c3, mn3, mx3))) => {
          c3 must_== c1 + c2
          mn3 must_== (mn1 min mn2)
          mx3 must_== (mx1 max mx2)
        }
        case (StringValueStats(c1, mn1, mx1), StringValueStats(c2, mn2, mx2), Some(StringValueStats(c3, mn3, mx3))) => {
          c3 must_== c1 + c2
          mn3 must_== Order[String].min(mn1, mn2)
          mx3 must_== Order[String].max(mx1, mx2)
        }
        case (e1, e2, r) => r must beNone
      }
    }
  }

  "metadata maps" should {
    "survive round trip serialization" in check { in: Map[MetadataType, Metadata] =>
      in.map(_._2).toList.serialize.validated[List[Metadata]] must beLike {
        case Success(out) => in must_== Map[MetadataType, Metadata](out.map{ m => (m.metadataType, m) }: _*)
      }
    }

    "merge as expected" in check { (sample1: List[Map[MetadataType, Metadata]], sample2: List[Map[MetadataType, Metadata]]) =>
      val prepared = sample1 zip sample2 map {
        case (s1, s2) => (s1, s2, s1 |+| s2)
      }
      
      forall(prepared) {
        case (s1, s2, r) => {
          val keys = s1.keys ++ s2.keys

          forall(keys) { k =>
            (s1.get(k), s2.get(k)) must beLike {
              case (Some(a), Some(b)) => r(k) must_== a.merge(b).get
              case (Some(a), _)       => r(k) must_== a
              case (_, Some(b))       => r(k) must_== b
            }
          }
        }
      }
    }
  }
}

trait MetadataGenerators extends util.ArbitraryJValue {
  import Gen._
  import Arbitrary._
  
  implicit val arbMetadata: Arbitrary[Metadata] = Arbitrary(genMetadata)
  implicit val arbMetadataMap: Arbitrary[Map[MetadataType, Metadata]] = Arbitrary(genMetadataMap)

  val metadataGenerators = List[Gen[Metadata]](genBooleanMetadata, genLongMetadata, genDoubleMetadata, genBigDecimalMetadata, genStringMetadata)

  def genMetadataList: Gen[List[Metadata]] = for(cnt <- choose(0,10); l <- listOfN(cnt, genMetadata)) yield { l } 

  def genMetadataMap: Gen[Map[MetadataType, Metadata]] = genMetadataList map { l => Map( l.map( m => (m.metadataType, m) ): _* ) }

  def genMetadata: Gen[Metadata] = frequency( metadataGenerators.map { (1, _) }: _* )

  def genBooleanMetadata: Gen[BooleanValueStats] = for(count <- choose(0, 1000); trueCount <- choose(0, count)) yield BooleanValueStats(count, trueCount)
  def genLongMetadata: Gen[LongValueStats] = for(count <- choose(0, 1000); a <- arbLong.arbitrary; b <- arbLong.arbitrary) yield LongValueStats(count, a min b,a max b)
  def genDoubleMetadata: Gen[DoubleValueStats] = for(count <- choose(0, 1000); a <- arbDouble.arbitrary; b <- arbDouble.arbitrary) yield DoubleValueStats(count, a min b,a max b)
  def genBigDecimalMetadata: Gen[BigDecimalValueStats] = for(count <- choose(0, 1000); a <- arbBigDecimal.arbitrary; b <- arbBigDecimal.arbitrary) yield BigDecimalValueStats(count, a min b, a max b)
  def genStringMetadata: Gen[StringValueStats] = for(count <- choose(0, 1000); a <- arbString.arbitrary; b <- arbString.arbitrary) yield StringValueStats(count, Order[String].min(a,b), Order[String].max(a,b))
}
