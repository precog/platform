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

case class Event(path: String, content: Set[(JPath, (JValue, Set[Metadata]))]) 

class EventSerialization {

  def simpleFlatten(jvalue: JValue): Set[(JPath, JValue)] = {
    jvalue.flattenWithPath.toSet
  }

  def dataRepresentation(content: Set[(JPath, (JValue, Set[Metadata]))]): JValue = {
    content.foldLeft[JValue](JObject(Nil)){ (acc, v) => acc.set(v._1, v._2._1) }
  }

  def metadataRepresentation(content: Set[(JPath, (JValue, Set[Metadata]))]): JValue = {
    content.foldLeft[JValue](JObject(Nil)){ (acc, v) => acc.set(v._1, v._2._2.serialize) }
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
