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

import com.reportgrid.analytics.Path

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._

import akka.actor._
import akka.actor.Actor._
import akka.routing._
import akka.dispatch.Future

import scala.collection.mutable.{Map => MMap, ListBuffer}

import scalaz._
import scalaz.Scalaz._

object JPathSerialization {
  implicit val JPathDecomposer : Decomposer[JPath] = new Decomposer[JPath] {
    def decompose(jpath: JPath) : JValue = jpath.toString.serialize
  }

  implicit val JPathExtractor : Extractor[JPath] = new Extractor[JPath] with ValidatedExtraction[JPath] {
    override def validated(obj : JValue) : Validation[Error,JPath] = 
      obj.validated[String].map(JPath(_))
  }
}

sealed trait QualifiedSelector {
  def path: Path
  def selector: JPath
  def valueType: PrimitiveType
}

trait QualifiedSelectorSerialization {

  import JPathSerialization._

  implicit val QualifiedSelectorDecomposer : Decomposer[QualifiedSelector] = new Decomposer[QualifiedSelector] {
    def decompose(selector : QualifiedSelector) : JValue = JObject (
      List(JField("path", selector.path.serialize),
           JField("selector", selector.selector.serialize),
           JField("valueType", selector.valueType.serialize))
    )
  }

  implicit val QualifiedSelectorExtractor : Extractor[QualifiedSelector] = new Extractor[QualifiedSelector] with ValidatedExtraction[QualifiedSelector] {
    override def validated(obj : JValue) : Validation[Error,QualifiedSelector] = 
      ((obj \ "path").validated[Path] |@|
       (obj \ "selector").validated[JPath] |@|
       (obj \ "valueType").validated[PrimitiveType]).apply(QualifiedSelector(_,_,_))
  }
}

object QualifiedSelector extends QualifiedSelectorSerialization {
  private case class QualifiedSelectorImpl(path: Path, selector: JPath, valueType: PrimitiveType) extends QualifiedSelector
  def apply(path: Path, selector: JPath, valueType: PrimitiveType): QualifiedSelector = QualifiedSelectorImpl(path, selector, valueType)
  def unapply(qs: QualifiedSelector): Option[(Path, JPath, PrimitiveType)] = Some((qs.path, qs.selector, qs.valueType))
}


case class BoundMetadata(qsel: QualifiedSelector, metadata: Set[Metadata]) extends QualifiedSelector {
  def path = qsel.path
  def selector = qsel.selector
  def valueType = qsel.valueType
}

trait BoundMetadataSerialization {

  import JPathSerialization._

  implicit val BoundMetadataDecomposer : Decomposer[BoundMetadata] = new Decomposer[BoundMetadata] {
    def decompose(selector : BoundMetadata) : JValue = JObject (
      List(JField("qualifiedSelector", selector.selector.serialize),
           JField("metadata", selector.metadata.serialize))
    )
  }

  implicit val BoundMetadataExtractor : Extractor[BoundMetadata] = new Extractor[BoundMetadata] with ValidatedExtraction[BoundMetadata] {
    override def validated(obj : JValue) : Validation[Error,BoundMetadata] = 
      ((obj \ "qualifiedSelector").validated[QualifiedSelector] |@|
       (obj \ "metadata").validated[Set[Metadata]]).apply(BoundMetadata(_,_))
  }
}

object BoundMetadata extends BoundMetadataSerialization

case class ProjectionDescriptor(columns: Seq[QualifiedSelector], metadata: Set[Metadata])

trait ProjectionDescriptorSerialization {
  implicit val ProjectionDescriptorDecomposer : Decomposer[ProjectionDescriptor] = new Decomposer[ProjectionDescriptor] {
    def decompose(descriptor : ProjectionDescriptor) : JValue = JObject (
      List(JField("columns", descriptor.columns.serialize),
           JField("metadata", descriptor.metadata.serialize))
    )
  }

  implicit val ProjectionDescriptorExtractor : Extractor[ProjectionDescriptor] = new Extractor[ProjectionDescriptor] with ValidatedExtraction[ProjectionDescriptor] {
    override def validated(obj : JValue) : Validation[Error,ProjectionDescriptor] = 
      ((obj \ "columns").validated[List[QualifiedSelector]] |@| 
       (obj \ "metadata").validated[Set[Metadata]]).apply(ProjectionDescriptor(_,_))
  }
}

object ProjectionDescriptor extends ProjectionDescriptorSerialization

