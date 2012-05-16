package com.precog.common

import blueeyes.json.JPath
import blueeyes.json.JsonAST._

import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization.{DateTimeDecomposer => _, DateTimeExtractor => _, _}
import blueeyes.json.xschema.Extractor._

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scalaz._
import Scalaz._

package object security {
  type TokenID = String
  type GrantID = String
  type UID = String

  private val isoFormat = ISODateTimeFormat.dateTime

  implicit val TZDateTimeDecomposer: Decomposer[DateTime] = new Decomposer[DateTime] {
    override def decompose(d: DateTime): JValue = JString(isoFormat.print(d))
  }

  implicit val TZDateTimeExtractor: Extractor[DateTime] = new Extractor[DateTime] with ValidatedExtraction[DateTime] {    
    override def validated(obj: JValue): Validation[Error, DateTime] = obj match {
      case JString(dt) => Success(isoFormat.parseDateTime(dt))
      case _           => Failure(Invalid("Date time must be represented as JSON string"))
    }
  }

  implicit val OptionDateTimeDecomposer: Decomposer[Option[DateTime]] = OptionDecomposer[DateTime]
  implicit val OptionDateTimeExtractor: Extractor[Option[DateTime]] = OptionExtractor[DateTime]
   
  val tidSafePrefix = 30 
  val gidSafePrefix = 30 

  def safeTokenID(tid: TokenID): String = safePrefix(tid, tidSafePrefix) 
  def safeGrantID(gid: GrantID): String = safePrefix(gid, gidSafePrefix) 

  def safePrefix(s: String, prefix: Int): String = s.substring(0, math.min(s.length-1, prefix))
}
