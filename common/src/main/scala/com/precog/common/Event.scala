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

import blueeyes.json._
import blueeyes.json.serialization.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._

import scalaz._
import scalaz.syntax.apply._

sealed trait Action

case class Event(path: Path, apiKey: String, data: JValue, metadata: Map[JPath, Set[UserMetadata]]) extends Action 

class EventSerialization {

  implicit val EventDecomposer: Decomposer[Event] = new Decomposer[Event] {
    override def decompose(event: Event): JValue = JObject(
      List(
        JField("path", event.path.serialize),
        JField("tokenId", event.apiKey.serialize),
        JField("data", event.data),
        JField("metadata", event.metadata.serialize)))
  }

  implicit val EventExtractor: Extractor[Event] = new Extractor[Event] with ValidatedExtraction[Event] {
    override def validated(obj: JValue): Validation[Error, Event] = 
      ((obj \ "path").validated[Path] |@|
       (obj \ "tokenId").validated[String] |@|
       (obj \ "metadata").validated[Map[JPath, Set[UserMetadata]]]).apply(Event(_,_,obj \ "data",_))
  }  

}

object Event extends EventSerialization {
  def fromJValue(path: Path, data: JValue, ownerAPIKey: String): Event = {
    Event(path, ownerAPIKey, data, Map[JPath, Set[UserMetadata]]())
  }
}

case class Archive(path: Path, apiKey: String) extends Action

class ArchiveSerialization {

  implicit val ArchiveDecomposer: Decomposer[Archive] = new Decomposer[Archive] {
    override def decompose(archive: Archive): JValue = JObject(
      List(
        JField("path", archive.path.serialize),
        JField("tokenId", archive.apiKey.serialize)))
  }

  implicit val ArchiveExtractor: Extractor[Archive] = new Extractor[Archive] with ValidatedExtraction[Archive] {
    override def validated(obj: JValue): Validation[Error, Archive] = 
      ((obj \ "path").validated[Path] |@|
       (obj \ "tokenId").validated[String]).apply(Archive(_,_))
  }  
}

object Archive extends ArchiveSerialization


// vim: set ts=4 sw=4 et:
