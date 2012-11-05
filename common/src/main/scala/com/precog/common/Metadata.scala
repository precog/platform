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
package com.precog.common

import java.nio.ByteBuffer
import java.nio.charset.Charset

import scala.math.Ordering
import scala.collection.mutable

import blueeyes.json._
import blueeyes.json.serialization.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._

import scalaz._
import scalaz.syntax.apply._

sealed trait MetadataType

object MetadataType {
  def toName(metadataType: MetadataType): String = metadataType match {
    case BooleanValueStats => "BooleanValueStats"
    case LongValueStats => "LongValueStats"
    case DoubleValueStats => "DoubleValueStats"
    case BigDecimalValueStats => "BigDecimalValueStats"
    case StringValueStats => "StringValueStats"
  }

  def fromName(name: String): Option[MetadataType] = name match {
    case "BooleanValueStats" => Option(BooleanValueStats)
    case "LongValueStats" => Option(LongValueStats)
    case "DoubleValueStats" => Option(DoubleValueStats)
    case "BigDecimalValueStats" => Option(BigDecimalValueStats)
    case "StringValueStats" => Option(StringValueStats)
    case _ => None
  }
}


sealed trait Metadata {
  def metadataType: MetadataType

  def merge(that: Metadata): Option[Metadata]
}


trait MetadataSerialization {
  implicit val MetadataDecomposer: Decomposer[Metadata] = new Decomposer[Metadata] {
    override def decompose(metadata: Metadata): JValue = {
      val value = metadata match {
        case bv @ BooleanValueStats(_,_)       => bv.serialize(BooleanValueStats.BooleanValueStatsDecomposer)
        case lv @ LongValueStats(_,_,_)        => lv.serialize(LongValueStats.LongValueStatsDecomposer)
        case dv @ DoubleValueStats(_,_,_)      => dv.serialize(DoubleValueStats.DoubleValueStatsDecomposer) 
        case bdv @ BigDecimalValueStats(_,_,_) => bdv.serialize(BigDecimalValueStats.BigDecimalValueStatsDecomposer)
        case sv @ StringValueStats(_,_,_)      => sv.serialize(StringValueStats.StringValueStatsDecomposer)
      }

      JObject(List(JField(MetadataType.toName(metadata.metadataType), value)))
    }
  }

  implicit val MetadataExtractor: Extractor[Metadata] = new Extractor[Metadata] with ValidatedExtraction[Metadata] {
   
    override def validated(obj: JValue): Validation[Error, Metadata] = obj match {
      case metadata @ JObject(entries) if entries.size == 1 => {
        val List((key, value)) = entries.toList
        MetadataType.fromName(key).map {
          case BooleanValueStats     => value.validated[BooleanValueStats] 
          case LongValueStats        => value.validated[LongValueStats] 
          case DoubleValueStats      => value.validated[DoubleValueStats] 
          case BigDecimalValueStats  => value.validated[BigDecimalValueStats] 
          case StringValueStats      => value.validated[StringValueStats] 
        } getOrElse { Failure(Invalid("Unknown metadata type: " + key)) }
      }
      case _ => Failure(Invalid("Invalid metadata entry: " + obj))
    }
  }
}

object Metadata extends MetadataSerialization {
  def toTypedMap(set: Set[Metadata]): Map[MetadataType, Metadata] = {
    set.foldLeft(Map[MetadataType, Metadata]()) ( (acc, el) => acc + (el.metadataType -> el) ) 
  }

  implicit val MetadataSemigroup = new Semigroup[Map[MetadataType, Metadata]] {
    def append(m1: Map[MetadataType, Metadata], m2: => Map[MetadataType, Metadata]) =
      m1.foldLeft(m2) { (acc, t) =>
        val (mtype, meta) = t
        acc + (mtype -> acc.get(mtype).map( combineMetadata(_,meta) ).getOrElse(meta))
      }

    def combineMetadata(m1: Metadata, m2: Metadata) = m1.merge(m2).getOrElse(sys.error("Invalid attempt to combine incompatible metadata"))
  }
}


// at present, no concrete subtypes of UserMetadata exist
sealed trait UserMetadata extends Metadata

trait UserMetadataSerialization {
  implicit val UserMetadataDecomposer: Decomposer[UserMetadata] = new Decomposer[UserMetadata] {
    override def decompose(metadata: UserMetadata): JValue = JUndefined
  }

  implicit val UserMetadataExtractor: Extractor[UserMetadata] = new Extractor[UserMetadata] with ValidatedExtraction[UserMetadata] {
    override def validated(obj: JValue): Validation[Error, UserMetadata] = Failure(Invalid("No known forms of user metadata")) 
  }
}

object UserMetadata extends UserMetadataSerialization 


sealed trait MetadataStats extends Metadata {
  def count: Long
}

case class BooleanValueStats(count: Long, trueCount: Long) extends MetadataStats {
  def falseCount: Long = count - trueCount
  def probability: Double = trueCount.toDouble / count

  def metadataType = BooleanValueStats

  def merge(that: Metadata) = that match {
    case BooleanValueStats(count, trueCount) => Some(BooleanValueStats(this.count + count, this.trueCount + trueCount))
    case _                                   => None
  }
}

trait BooleanValueStatsSerialization {
  implicit val BooleanValueStatsDecomposer: Decomposer[BooleanValueStats] = new Decomposer[BooleanValueStats] {
    override def decompose(metadata: BooleanValueStats): JValue = JObject(List(
      JField("count", metadata.count),
      JField("trueCount", metadata.trueCount)
    ))
  }

  implicit val BooleanValueStatsExtractor: Extractor[BooleanValueStats] = new Extractor[BooleanValueStats] with ValidatedExtraction[BooleanValueStats] {
    override def validated(obj: JValue): Validation[Error, BooleanValueStats] = 
      ((obj \ "count").validated[Long] |@|
       (obj \ "trueCount").validated[Long]).apply(BooleanValueStats(_, _))
  }
}

object BooleanValueStats extends MetadataType with BooleanValueStatsSerialization


case class LongValueStats(count: Long, min: Long, max: Long) extends MetadataStats {
  def metadataType = LongValueStats

  def merge(that: Metadata) = that match {
    case LongValueStats(count, min, max) => Some(LongValueStats(this.count + count, this.min.min(min), this.max.max(max)))
    case _                               => None
  }
}

trait LongValueStatsSerialization {
  implicit val LongValueStatsDecomposer: Decomposer[LongValueStats] = new Decomposer[LongValueStats] {
    override def decompose(metadata: LongValueStats): JValue = JObject(List(
      JField("count", metadata.count),
      JField("min", metadata.min),
      JField("max", metadata.max)
    ))
  }

  implicit val LongValueStatsExtractor: Extractor[LongValueStats] = new Extractor[LongValueStats] with ValidatedExtraction[LongValueStats] {
    override def validated(obj: JValue): Validation[Error, LongValueStats] = 
      ((obj \ "count").validated[Long] |@|
       (obj \ "min").validated[Long] |@|
       (obj \ "max").validated[Long]).apply(LongValueStats(_, _, _))
  }
}

object LongValueStats extends MetadataType with LongValueStatsSerialization


case class DoubleValueStats(count: Long, min: Double, max: Double) extends MetadataStats {
  def metadataType = DoubleValueStats
  
  def merge(that: Metadata) = that match {
    case DoubleValueStats(count, min, max) => Some(DoubleValueStats(this.count + count, this.min min min, this.max max max))
    case _                                 => None
  }
}

trait DoubleValueStatsSerialization {
  implicit val DoubleValueStatsDecomposer: Decomposer[DoubleValueStats] = new Decomposer[DoubleValueStats] {
    override def decompose(metadata: DoubleValueStats): JValue = JObject(List(
      JField("count", metadata.count),
      JField("min", metadata.min),
      JField("max", metadata.max)
    ))
  }

  implicit val DoubleValueStatsExtractor: Extractor[DoubleValueStats] = new Extractor[DoubleValueStats] with ValidatedExtraction[DoubleValueStats] {
    override def validated(obj: JValue): Validation[Error, DoubleValueStats] = 
      ((obj \ "count").validated[Long] |@|
       (obj \ "min").validated[Double] |@|
       (obj \ "max").validated[Double]).apply(DoubleValueStats(_, _, _))
  }
}

object DoubleValueStats extends MetadataType with DoubleValueStatsSerialization


case class BigDecimalValueStats(count: Long, min: BigDecimal, max: BigDecimal) extends MetadataStats {
  def metadataType = BigDecimalValueStats 
  
  def merge(that: Metadata) = that match {
    case BigDecimalValueStats(count, min, max) => Some(BigDecimalValueStats(this.count + count, this.min min min, this.max max max))
    case _                                 => None
  }
}

trait BigDecimalValueStatsSerialization {
  implicit val BigDecimalValueStatsDecomposer: Decomposer[BigDecimalValueStats] = new Decomposer[BigDecimalValueStats] {
    override def decompose(metadata: BigDecimalValueStats): JValue = JObject(List(
      JField("count", metadata.count),
      JField("min", metadata.min),
      JField("max", metadata.max)
    ))
  }

  implicit val BigDecimalValueStatsExtractor: Extractor[BigDecimalValueStats] = new Extractor[BigDecimalValueStats] with ValidatedExtraction[BigDecimalValueStats] {
    override def validated(obj: JValue): Validation[Error, BigDecimalValueStats] = 
      ((obj \ "count").validated[Long] |@|
       (obj \ "min").validated[BigDecimal] |@|
       (obj \ "max").validated[BigDecimal]).apply(BigDecimalValueStats(_, _, _))
  }
}

object BigDecimalValueStats extends MetadataType with BigDecimalValueStatsSerialization


case class StringValueStats(count: Long, min: String, max: String) extends MetadataStats {

  def metadataType = StringValueStats 
  
  def merge(that: Metadata) = that match {
    case StringValueStats(count, min, max) => Some(StringValueStats(this.count + count, 
                                                                    Order[String].min(this.min, min), 
                                                                    Order[String].max(this.max, max)))
    case _                                 => None
  }
}

trait StringValueStatsSerialization {
  implicit val StringValueStatsDecomposer: Decomposer[StringValueStats] = new Decomposer[StringValueStats] {
    override def decompose(metadata: StringValueStats): JValue = JObject(List(
      JField("count", metadata.count),
      JField("min", metadata.min),
      JField("max", metadata.max)
    ))
  }

  implicit val StringValueStatsExtractor: Extractor[StringValueStats] = new Extractor[StringValueStats] with ValidatedExtraction[StringValueStats] {
    override def validated(obj: JValue): Validation[Error, StringValueStats] = 
      ((obj \ "count").validated[Long] |@|
       (obj \ "min").validated[String] |@|
       (obj \ "max").validated[String]).apply(StringValueStats(_, _, _))
  }
}

object StringValueStats extends MetadataType with StringValueStatsSerialization
