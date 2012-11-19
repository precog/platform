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
package com.precog.heimdall

import blueeyes.core.http.{ MimeType, MimeTypes }

import blueeyes.json._
import blueeyes.json.serialization.{ Decomposer, Extractor, ValidatedExtraction }
import blueeyes.json.serialization.DefaultSerialization._

import org.apache.commons.codec.binary.Base64

import scalaz._

case class JobResult(mimeTypes: List[MimeType], content: Array[Byte]) {
  override def hashCode: Int = mimeTypes.## * 23 + content.toList.##

  override def equals(that: Any): Boolean = that match {
    case JobResult(thoseMimeTypes, thatContent) =>
      val len = content.length
      (mimeTypes.toSet == thoseMimeTypes.toSet) && (len == thatContent.length) && {
        var i = 0
        var result = true
        while (result && i < len) {
          result = content(i) == thatContent(i)
          i += 1
        }
        result
      }

    case _ =>
      false
  }
}

object JobResult extends JobResultSerialization

trait JobResultSerialization {
  import scalaz.syntax.apply._
  import scalaz.syntax.monad._
  import Validation._

  implicit object JobResultDecomposer extends Decomposer[JobResult] {
    override def decompose(result: JobResult): JValue = JObject(List(
      JField("content", JString(Base64.encodeBase64String(result.content))),
      JField("mimeTypes", JArray(result.mimeTypes map { mimeType =>
        JString(mimeType.value)
      }))
    ))
  }

  implicit object JobResultExtractor extends Extractor[JobResult] with ValidatedExtraction[JobResult] {
    import Extractor._

    override def validated(obj: JValue): Validation[Error, JobResult] = {
      val mimeTypes = (obj \ "mimeTypes").validated[List[String]] flatMap { rawTypes =>
        success[Error, List[MimeType]]((rawTypes flatMap (MimeTypes.parseMimeTypes(_))).toList)
      }
      (mimeTypes |@| (obj \ "content").validated[String]) { (mimeTypes, content) =>
        JobResult(mimeTypes, Base64.decodeBase64(content))
      }
    }
  }
}

