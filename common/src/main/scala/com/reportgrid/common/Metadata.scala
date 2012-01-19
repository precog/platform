package com.reportgrid.common

import java.nio.ByteBuffer
import java.nio.charset.Charset

import scala.math.Ordering
import scala.collection.mutable.{Map => MMap}

import blueeyes.json.JsonAST._
import blueeyes.json.JPath
import blueeyes.json.JsonParser
import blueeyes.json.Printer

import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import scalaz._
import Scalaz._

sealed trait MetadataType

sealed abstract trait Metadata {
  def merge(other: Metadata): Option[Metadata]
}

trait MetadataSerialization {
  implicit val MetadataDecomposer: Decomposer[Metadata] = new Decomposer[Metadata] {
    override def decompose(metadata: Metadata): JValue = metadata match {
      case owners @ Ownership(_) => Ownership.OwnershipDecomposer.apply(owners)
    }
  }

  implicit val MetadataExtractor: Extractor[Metadata] = new Extractor[Metadata] with ValidatedExtraction[Metadata] {
    override def validated(obj: JValue): Validation[Error, Metadata] = obj match {
      case metadata @ JObject(JField(key, _) :: Nil) => key match {
        case "ownership" => Ownership.OwnershipExtractor.validated(metadata)
        case _           => Failure(Invalid("Unknown metadata type: " + key))
      }
      case _                                         => Failure(Invalid("Invalid metadata entry: " + obj))
    }
  }
}

object Metadata extends MetadataSerialization {
  def toTypedMap(set: Set[Metadata]): MMap[MetadataType, Metadata] = {
    set.foldLeft(MMap[MetadataType, Metadata]()) ( (acc, el) => acc + (typeOf(el) -> el) ) 
  }

  def typeOf(metadata: Metadata): MetadataType = metadata match {
    case Ownership(_)                  => Ownership
    case BooleanValueStats(_, _)       => BooleanValueStats
    case LongValueStats(_, _, _)       => LongValueStats
    case DoubleValueStats(_, _, _)     => DoubleValueStats
    case BigDecimalValueStats(_, _, _) => BigDecimalValueStats
    case StringValueStats(_, _, _)     => StringValueStats
  }

  def valueStats(jval: JValue): Option[Metadata] = typedValueStats(jval).map( _._2 )

  def typedValueStats(jval: JValue): Option[(MetadataType, Metadata)] = jval match {
    case JBool(b)     => Some((BooleanValueStats, BooleanValueStats(2, if(b) 1 else 0)))
    case JInt(i)      => Some((BigDecimalValueStats, BigDecimalValueStats(1, BigDecimal(i), BigDecimal(i))))
    case JDouble(d)   => Some((DoubleValueStats, DoubleValueStats(1, d, d)))
    case JString(s)   => Some((StringValueStats, StringValueStats(1, s, s)))
    case _            => None
  }

  implicit val OwnershipSemigroup = new Semigroup[Ownership] {
    def append(o1: Ownership, o2: => Ownership) = Ownership(o1.owners ++ o2.owners) 
  }

  implicit val BooleanValueStatsSemigroup = new Semigroup[BooleanValueStats] {
    def append(bv1: BooleanValueStats, bv2: => BooleanValueStats) = BooleanValueStats(bv1.count + bv2.count, bv1.trueCount + bv2.trueCount)
  }

  implicit val LongValueStatsSemigroup = new Semigroup[LongValueStats] {
    def append(lv1: LongValueStats, lv2: => LongValueStats) = 
      LongValueStats(lv1.count + lv2.count, Ordering[Long].min(lv1.min, lv2.min), Ordering[Long].max(lv1.max, lv2.max))
  }

  implicit val DoubleValueStatsSemigroup = new Semigroup[DoubleValueStats] {
    def append(lv1: DoubleValueStats, lv2: => DoubleValueStats) = 
      DoubleValueStats(lv1.count + lv2.count, Ordering[Double].min(lv1.min, lv2.min), Ordering[Double].max(lv1.max, lv2.max))
  }
  
  implicit val BigDecimalValueStatsSemigroup = new Semigroup[BigDecimalValueStats] {
    def append(lv1: BigDecimalValueStats, lv2: => BigDecimalValueStats) = 
      BigDecimalValueStats(lv1.count + lv2.count, Ordering[BigDecimal].min(lv1.min, lv2.min), Ordering[BigDecimal].max(lv1.max, lv2.max))
  }
  
  implicit val StringValueStatsSemigroup = new Semigroup[StringValueStats] {
    def append(lv1: StringValueStats, lv2: => StringValueStats) = 
      StringValueStats(lv1.count + lv2.count, Ordering[String].min(lv1.min, lv2.min), Ordering[String].max(lv1.max, lv2.max))
  }
  
  implicit val MetadataSemigroup = new Semigroup[MMap[MetadataType, Metadata]] {
    def append(m1: MMap[MetadataType, Metadata], m2: => MMap[MetadataType, Metadata]) =
      m1.foldLeft(m2) { (acc, t) =>
        val (mtype, meta) = t
        acc + (mtype -> acc.get(mtype).map( combineMetadata(_,meta) ).getOrElse(meta))
      }

    def combineMetadata(m1: Metadata, m2: Metadata) = (m1, m2) match {
      case (o1 @ Ownership(_), o2 @ Ownership(_))                                    => o1 |+| o2
      case (bv1 @ BooleanValueStats(_, _), bv2 @ BooleanValueStats(_, _))            => bv1 |+| bv2
      case (lv1 @ LongValueStats(_,_,_), lv2 @ LongValueStats(_,_,_))                => lv1 |+| lv2
      case (dv1 @ DoubleValueStats(_,_,_), dv2 @ DoubleValueStats(_,_,_))            => dv1 |+| dv2
      case (bdv1 @ BigDecimalValueStats(_,_,_), bdv2 @ BigDecimalValueStats(_,_,_))  => bdv1 |+| bdv2
      case (sv1 @ StringValueStats(_,_,_), sv2 @ StringValueStats(_,_,_))            => sv1 |+| sv2
      case _                                                                         => error("Invalid attempt to combine incompatible metadata")
    }
  }
}

case class Ownership(owners: Set[String]) extends Metadata {
  def merge(other: Metadata) = (this, other) match {
    case (Ownership(os1), Ownership(os2)) => Some(Ownership(os1 ++ os2))
    case _                                => None
  }
}

trait OwnershipSerialization {
  implicit val OwnershipDecomposer: Decomposer[Ownership] = new Decomposer[Ownership] {
    override def decompose(ownership: Ownership): JValue = {
      JObject(JField("ownership", JArray(ownership.owners.map(JString(_)).toList)) :: Nil)
    }
  }

  implicit val OwnershipExtractor: Extractor[Ownership] = new Extractor[Ownership] with ValidatedExtraction[Ownership] {
    override def validated(obj: JValue): Validation[Error, Ownership] =
      (obj \ "ownership").validated[Set[String]].map(Ownership(_))
  }
}

object Ownership extends MetadataType with OwnershipSerialization with Function1[Set[String], Ownership]


case class BooleanValueStats(count: Long, trueCount: Long) extends Metadata {
  def falseCount: Long = count - trueCount
  def probability: Double = trueCount.toDouble / count
  def merge(other: Metadata) = (this, other) match {
    case (BooleanValueStats(c1, t1), BooleanValueStats(c2, t2)) => Some(BooleanValueStats(c1+c2,t1+t2))
    case _                                                      => None
  }
}

trait BooleanValueStatsSerialization {

}

object BooleanValueStats extends MetadataType with BooleanValueStatsSerialization with Function2[Long, Long, BooleanValueStats]

case class LongValueStats(count: Long, min: Long, max: Long) extends Metadata {
  def merge(other: Metadata) = (this, other) match {
    case (LongValueStats(c1, min1, max1), LongValueStats(c2, min2, max2)) =>
      Some(LongValueStats(c1+c2, Ordering[Long].min(min1, min2), Ordering[Long].max(max1, max2)))  
    case _ => None
  }
}

trait LongValueStatsSerialization {

}

object LongValueStats extends MetadataType with LongValueStatsSerialization with Function3[Long, Long, Long, LongValueStats]

case class DoubleValueStats(count: Long, min: Double, max: Double) extends Metadata {
  def merge(other: Metadata) = (this, other) match {
    case (DoubleValueStats(c1, min1, max1), DoubleValueStats(c2, min2, max2)) =>
      Some(DoubleValueStats(c1+c2, Ordering[Double].min(min1, min2), Ordering[Double].max(max1, max2)))  
    case _ => None
  }
}

trait DoubleValueStatsSerialization {

}

object DoubleValueStats extends MetadataType with DoubleValueStatsSerialization with Function3[Long, Double, Double, DoubleValueStats]

case class BigDecimalValueStats(count: Long, min: BigDecimal, max: BigDecimal) extends Metadata {
  def merge(other: Metadata) = (this, other) match {
    case (BigDecimalValueStats(c1, min1, max1), BigDecimalValueStats(c2, min2, max2)) =>
      Some(BigDecimalValueStats(c1+c2, Ordering[BigDecimal].min(min1, min2), Ordering[BigDecimal].max(max1, max2)))  
    case _ => None
  }
}

trait BigDecimalValueStatsSerialization {

}

object BigDecimalValueStats extends MetadataType with BigDecimalValueStatsSerialization with Function3[Long, BigDecimal, BigDecimal, BigDecimalValueStats]

case class StringValueStats(count: Long, min: String, max: String) extends Metadata {
  def merge(other: Metadata) = (this, other) match {
    case (StringValueStats(c1, min1, max1), StringValueStats(c2, min2, max2)) =>
      Some(StringValueStats(c1+c2, Ordering[String].min(min1, min2), Ordering[String].max(max1, max2)))  
    case _ => None
  }
}

trait StringValueStatsSerialization {

}

object StringValueStats extends MetadataType with StringValueStatsSerialization with Function3[Long, String, String, StringValueStats]

/** Sorting Metadata */
sealed trait SortOrder

case object AscendingOrder extends SortOrder
case object DescendingOrder extends SortOrder

case class ColumnSorting(sortOrder: SortOrder) extends Metadata {
  def merge(other: Metadata) = sys.error("todo")
}
