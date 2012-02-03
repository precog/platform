package com.precog.common

import java.nio.ByteBuffer
import java.nio.charset.Charset

import blueeyes.json.JsonAST._
import blueeyes.json.JPath
import blueeyes.json.JsonParser
import blueeyes.json.Printer

import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import com.precog.analytics._
import com.precog.common.util.FixMe._

import scalaz._
import Scalaz._

case class Event(path: Path, tokenId: String, data: JValue, metadata: Map[JPath, Set[UserMetadata]]) 

class EventSerialization {

  implicit val EventDecomposer: Decomposer[Event] = new Decomposer[Event] {
    override def decompose(event: Event): JValue = JObject(
      List(
        JField("path", event.path.serialize),
        JField("tokenId", event.tokenId.serialize),
        JField("data", event.data),
        sys.error("todo")))
        //JField("metadata", event.metadata.serialize)))
  }

  implicit val EventExtractor: Extractor[Event] = new Extractor[Event] with ValidatedExtraction[Event] {
    override def validated(obj: JValue): Validation[Error, Event] = 
      sys.error("todo")
      //((obj \ "path").validated[Path] |@|
      // (obj \ "tokenId").validated[String] |@|
      // (obj \ "metadata").validated[Map[JPath, Set[UserMetadata]]]).apply(Event(_,_,obj \ "value",_))
  }  

}

object Event extends EventSerialization {
  def fromJValue(path: Path, data: JValue, ownerToken: String): Event = {
    Event(path, ownerToken, data, Map[JPath, Set[UserMetadata]]())
  }
}

// vim: set ts=4 sw=4 et:
