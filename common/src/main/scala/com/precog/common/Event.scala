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

case class Event(path: Path, content: Set[(JPath, (JValue, Set[Metadata]))]) 

class EventSerialization {

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
   
    data.flattenWithPath.toSet[(JPath, JValue)].map { t => attachMetadata(t._1, t._2, extractMetadataForPath(t._1, allMetadata)) } 
  }

  implicit val EventExtractor: Extractor[Event] = new Extractor[Event] with ValidatedExtraction[Event] {
    override def validated(obj: JValue): Validation[Error, Event] = 
      ((obj \ "path").validated[Path] |@|
        ((obj \ "metadata").validated[JValue] |@|
         (obj \ "data").validated[JValue]).apply(representationsToContent(_, _))).apply(Event(_, _))
  }  
}

object Event extends EventSerialization {
  def fromJValue(path: Path, data: JValue, ownerToken: String): Event = {
    def assignOwnership(properties: Set[(JPath, JValue)]): Set[(JPath, (JValue, Set[Metadata]))] = properties.map { 
      fixme("Null ownership being attributed at this point in time")
      (t: (JPath, JValue)) => { (t._1, (t._2, Set())) }: (JPath, (JValue, Set[Metadata]))
    }
    Event(path, assignOwnership(data.flattenWithPath.toSet))
  }

  def extractOwners(event: Event): Set[String] = {
    println("Ownership being ignored until fixed")
    Set.empty
  }
}

// vim: set ts=4 sw=4 et:
