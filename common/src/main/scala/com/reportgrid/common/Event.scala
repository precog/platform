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
package com.reportgrid.common

import java.nio.ByteBuffer
import java.nio.charset.Charset

import blueeyes.json.JsonAST._
import blueeyes.json.JPath
import blueeyes.json.JsonParser
import blueeyes.json.Printer

import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import scalaz._
import Scalaz._

//TODO: This should be (Path, JValue, JValue)
case class Event(path: String, content: Set[(JPath, (JValue, Set[Metadata]))]) 

class EventSerialization {

  def simpleFlatten(jvalue: JValue): Set[(JPath, JValue)] = {
    jvalue.flattenWithPath.toSet
  }

  def dataRepresentation(content: Set[(JPath, (JValue, Set[Metadata]))]): JValue = {
    JValue.unflatten( content.map( t => (t._1, t._2._1) ).toList )
  }

  def metadataRepresentation(content: Set[(JPath, (JValue, Set[Metadata]))]): JValue = {
    JValue.unflatten( content.map( t => (t._1, t._2._2.serialize)).toList )
  }

  implicit val EventDecomposer: Decomposer[Event] = new Decomposer[Event] {
    override def decompose(event: Event): JValue = JObject(
      List(
        JField("path", event.path.serialize),
        JField("metadata", metadataRepresentation(event.content)),
        JField("data", dataRepresentation(event.content))))
  }

  def representationsToContent(allMetadata: JValue, data: JValue): Set[(JPath, (JValue, Set[Metadata]))] = {
    
    def extractMetadataForPath(path: JPath, allMetadata: JValue): Set[Metadata] = {
      allMetadata(path) match {
        case JNothing => Set()
        case jv       => jv.validated[Set[Metadata]].fold(e => Set(), v => v)
      }
    }

    def attachMetadata(path: JPath, value: JValue, metadata: Set[Metadata]): (JPath, (JValue, Set[Metadata])) = {
      (path, (value, metadata))
    }
   
    simpleFlatten(data).map { t => attachMetadata(t._1, t._2, extractMetadataForPath(t._1, allMetadata)) } 
  }

  implicit val EventExtractor: Extractor[Event] = new Extractor[Event] with ValidatedExtraction[Event] {
    override def validated(obj: JValue): Validation[Error, Event] = 
      ((obj \ "path").validated[String] |@|
        ((obj \ "metadata").validated[JValue] |@|
         (obj \ "data").validated[JValue]).apply(representationsToContent(_, _))).apply(Event(_, _))
  }  
}

object Event extends EventSerialization {
  def fromJValue(path: String, data: JValue, ownerToken: String): Event = {
    def assignOwnership(properties: Set[(JPath, JValue)]): Set[(JPath, (JValue, Set[Metadata]))] = properties.map { 
      (t: (JPath, JValue)) => { (t._1, (t._2, Set(Ownership(Set(ownerToken))))) }: (JPath, (JValue, Set[Metadata]))
    }
    Event(path, assignOwnership(simpleFlatten(data).toSet))
  }

  def extractOwners(event: Event): Set[String] = {
    val result: Set[Set[String]] = event.content.flatMap(_._2._2.map {
      case Ownership(l) => l
    })
    result.flatten.toSet[String]
  }
}

// vim: set ts=4 sw=4 et:
