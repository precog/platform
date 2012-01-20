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
package com.reportgrid.yggdrasil

import com.reportgrid.common._
import com.reportgrid.analytics.Path

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JPath.{JPathDecomposer, JPathExtractor}
import blueeyes.json.xschema._
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._

import akka.actor._
import akka.actor.Actor._
import akka.routing._
import akka.dispatch.Future

import scala.collection.immutable.ListMap

import scalaz._
import scalaz.Scalaz._

sealed trait SortBy
case object ById extends SortBy
case object ByValue extends SortBy
case object ByValueThenId extends SortBy

case class QualifiedSelector(path: Path, selector: JPath, valueType: ColumnType) 

trait QualifiedSelectorSerialization {
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
       (obj \ "valueType").validated[ColumnType]).apply(QualifiedSelector(_,_,_))
  }
}

object QualifiedSelector extends QualifiedSelectorSerialization 
with ((Path, JPath, ColumnType) => QualifiedSelector)


case class ColumnDescriptor(qsel: QualifiedSelector, metadata: Set[Metadata]) 

trait ColumnDescriptorSerialization {
  implicit val ColumnDescriptorDecomposer : Decomposer[ColumnDescriptor] = new Decomposer[ColumnDescriptor] {
    def decompose(selector : ColumnDescriptor) : JValue = JObject (
      List(JField("qualifiedSelector", selector.qsel.serialize),
           JField("metadata", selector.metadata.serialize))
    )
  }

  implicit val ColumnDescriptorExtractor : Extractor[ColumnDescriptor] = new Extractor[ColumnDescriptor] with ValidatedExtraction[ColumnDescriptor] {
    override def validated(obj : JValue) : Validation[Error,ColumnDescriptor] = 
      ((obj \ "qualifiedSelector").validated[QualifiedSelector] |@|
       (obj \ "metadata").validated[Set[Metadata]]).apply(ColumnDescriptor(_,_))
  }
}

object ColumnDescriptor extends ColumnDescriptorSerialization


/** 
 * The descriptor for a projection 
 */
case class ProjectionDescriptor private (identiies: Int, indexedColumns: ListMap[ColumnDescriptor, Int], sorting: Seq[(ColumnDescriptor, SortBy)]) {
  lazy val columns = indexedColumns.map(_._1).toList 
}

trait ByteProjection {
  def descriptor: ProjectionDescriptor

  def project(id: Identities, v: Seq[CValue]): (Array[Byte], Array[Byte]) 
  def unproject[E](keyBytes: Array[Byte], valueBytes: Array[Byte])(f: (Identities, Seq[CValue]) => E): E
}

trait ProjectionDescriptorSerialization {
  implicit val ProjectionDescriptorDecomposer : Decomposer[ProjectionDescriptor] = new Decomposer[ProjectionDescriptor] {
    def decompose(descriptor : ProjectionDescriptor) : JValue = JObject (
      List(JField("columns", descriptor.columns.serialize))
    )
  }

  implicit val ProjectionDescriptorExtractor : Extractor[ProjectionDescriptor] = new Extractor[ProjectionDescriptor] with ValidatedExtraction[ProjectionDescriptor] {
    override def validated(obj : JValue) : Validation[Error,ProjectionDescriptor] = 
      sys.error("todo")
      //(obj \ "columns").validated[List[ColumnDescriptor]]
  }
}

object ProjectionDescriptor extends ProjectionDescriptorSerialization {
  def apply(columns: ListMap[ColumnDescriptor, Int], sorting: Seq[(ColumnDescriptor, SortBy)]): Validation[String, ProjectionDescriptor] = {
    val identities = columns.values.toSeq.sorted.foldLeft(Option(0)) {
      // test that identities are 0-based and sequential
      case (Some(cur), next) if cur == next => Some(next + 1)
      case _ => None
    }

    identities.toSuccess("Column identity indexes must be 0-based and must be sequential when sorted")
    .map(new ProjectionDescriptor(_, columns, sorting))
  }
}

