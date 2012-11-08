package com.precog.common

import blueeyes.json._
import blueeyes.json.serialization.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization.{DateTimeDecomposer => _, DateTimeExtractor => _, _}
import blueeyes.json.serialization.Extractor._

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scalaz._

package object security {
  type AccountID = String
  type APIKey    = String
  type GrantID   = String

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
}
