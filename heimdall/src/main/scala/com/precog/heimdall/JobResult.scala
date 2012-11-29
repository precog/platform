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

