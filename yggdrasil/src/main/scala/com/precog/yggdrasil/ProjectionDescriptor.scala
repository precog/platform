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
package com.precog.yggdrasil

import com.precog.common._

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

trait SortBySerialization {
  implicit val SortByDecomposer : Decomposer[SortBy] = new Decomposer[SortBy] {
    def decompose(sortBy: SortBy) : JValue = JString(toName(sortBy)) 
  }

  implicit val SortByExtractor : Extractor[SortBy] = new Extractor[SortBy] with ValidatedExtraction[SortBy] {
    override def validated(obj : JValue) : Validation[Error,SortBy] = obj match {
      case JString(s) => fromName(s).map(Success(_)).getOrElse(Failure(Invalid("Unknown SortBy property: " + s))) 
      case _          => Failure(Invalid("Expected JString type for SortBy property"))
    }
  }

  def fromName(s: String): Option[SortBy] = s match {
    case "ById"          => Some(ById)
    case "ByValue"       => Some(ByValue)
    case "ByValueThenId" => Some(ByValueThenId)
    case _               => None
  }

  def toName(sortBy: SortBy): String = sortBy match {
    case ById => "ById"
    case ByValue => "ByValue"
    case ByValueThenId => "ByValueThenId"
  }
}

object SortBy extends SortBySerialization

case class Authorities(uids: Set[String])

trait AuthoritiesSerialization {
  implicit val AuthoritiesDecomposer: Decomposer[Authorities] = new Decomposer[Authorities] {
    override def decompose(authorities: Authorities): JValue = {
      JObject(JField("uids", JArray(authorities.uids.map(JString(_)).toList)) :: Nil)
    }
  }

  implicit val AuthoritiesExtractor: Extractor[Authorities] = new Extractor[Authorities] with ValidatedExtraction[Authorities] {
    override def validated(obj: JValue): Validation[Error, Authorities] =
      (obj \ "uids").validated[Set[String]].map(Authorities(_))
  }
}

object Authorities extends AuthoritiesSerialization 

case class ColumnDescriptor(path: Path, selector: JPath, valueType: CType, authorities: Authorities) 

trait ColumnDescriptorSerialization {
  implicit val ColumnDescriptorDecomposer : Decomposer[ColumnDescriptor] = new Decomposer[ColumnDescriptor] {
    def decompose(selector : ColumnDescriptor) : JValue = JObject (
      List(JField("path", selector.path.serialize),
           JField("selector", selector.selector.serialize),
           JField("valueType", selector.valueType.serialize),
           JField("authorities", selector.authorities.serialize))
    )
  }

  implicit val ColumnDescriptorExtractor : Extractor[ColumnDescriptor] = new Extractor[ColumnDescriptor] with ValidatedExtraction[ColumnDescriptor] {
    override def validated(obj : JValue) : Validation[Error,ColumnDescriptor] = 
      ((obj \ "path").validated[Path] |@|
       (obj \ "selector").validated[JPath] |@|
       (obj \ "valueType").validated[CType] |@|
       (obj \ "authorities").validated[Authorities]).apply(ColumnDescriptor(_,_,_,_))
  }
}

object ColumnDescriptor extends ColumnDescriptorSerialization 
with ((Path, JPath, CType, Authorities) => ColumnDescriptor)



/** 
 * The descriptor for a projection 
 */
case class ProjectionDescriptor private (identities: Int, indexedColumns: ListMap[ColumnDescriptor, Int], sorting: Seq[(ColumnDescriptor, SortBy)]) {
  lazy val columns = indexedColumns.map(_._1).toList 
  lazy val selectors = columns.map(_.selector).toSet

  def columnAt(path: Path, selector: JPath) = columns.find(col => col.path == path && col.selector == selector)

  def satisfies(col: ColumnDescriptor) = columns.contains(col)

  override val hashCode: Int = scala.runtime.ScalaRunTime._hashCode(ProjectionDescriptor.this)
}

trait ProjectionDescriptorSerialization {

  case class IndexWrapper(colDesc: ColumnDescriptor, index: Int)

  trait IndexWrapperSerialization {
    implicit val IndexWrapperDecomposer : Decomposer[IndexWrapper] = new Decomposer[IndexWrapper] {
      def decompose(iw: IndexWrapper) : JValue = JObject (
        List(JField("descriptor", iw.colDesc.serialize),
             JField("index", iw.index.serialize))
      )
    }

    implicit val IndexWrapperExtractor : Extractor[IndexWrapper] = new Extractor[IndexWrapper] with ValidatedExtraction[IndexWrapper] {
      override def validated(obj : JValue) : Validation[Error,IndexWrapper] = {
        ((obj \ "descriptor").validated[ColumnDescriptor] |@|
         (obj \ "index").validated[Int]).apply(IndexWrapper(_, _))
      }
    }
  }

  object IndexWrapper extends IndexWrapperSerialization

  case class SortWrapper(colDesc: ColumnDescriptor, sortBy: SortBy)

  trait SortWrapperSerialization {
    implicit val SortWrapperDecomposer : Decomposer[SortWrapper] = new Decomposer[SortWrapper] {
      def decompose(sw: SortWrapper) : JValue = JObject (
        List(JField("descriptor", sw.colDesc.serialize),
             JField("sortBy", sw.sortBy.serialize))
      )
    }

    implicit val SortWrapperExtractor : Extractor[SortWrapper] = new Extractor[SortWrapper] with ValidatedExtraction[SortWrapper] {
      override def validated(obj : JValue) : Validation[Error,SortWrapper] = 
        ((obj \ "descriptor").validated[ColumnDescriptor] |@|
         (obj \ "sortBy").validated[SortBy]).apply(SortWrapper(_, _))
    }
  }

  object SortWrapper extends SortWrapperSerialization

  implicit val ProjectionDescriptorDecomposer : Decomposer[ProjectionDescriptor] = new Decomposer[ProjectionDescriptor] {
    def decompose(pd: ProjectionDescriptor) : JValue = JObject (
      JField("columns", pd.indexedColumns.toList.map( t => IndexWrapper(t._1, t._2) ).serialize) ::
      JField("sorting", pd.sorting.map( t => SortWrapper(t._1, t._2) ).serialize) :: 
      Nil
    )
  } 
  
  implicit val ProjectionDescriptorExtractor : Extractor[ProjectionDescriptor] = new Extractor[ProjectionDescriptor] with ValidatedExtraction[ProjectionDescriptor] { 
    override def validated(obj : JValue) : Validation[Error,ProjectionDescriptor] = 
      ((obj \ "columns").validated[List[IndexWrapper]].map( _.foldLeft(ListMap[ColumnDescriptor, Int]()) { (acc, el) => acc + (el.colDesc -> el.index) }) |@|
       (obj \ "sorting").validated[List[SortWrapper]].map( _.map{ sw => (sw.colDesc, sw.sortBy)})).apply( (_,_) ) match {
         case Success((cols, sorting)) => ProjectionDescriptor(cols, sorting).fold(e => Failure(Invalid(e)), v => Success(v))
         case Failure(e)               => Failure(e)
       }
  } 
}

object ProjectionDescriptor extends ProjectionDescriptorSerialization {

  def trustedApply(identities: Int, indexedColumns: ListMap[ColumnDescriptor, Int], sorting: Seq[(ColumnDescriptor, SortBy)]): ProjectionDescriptor = ProjectionDescriptor(identities, indexedColumns, sorting)

  def apply(indexedColumns: ListMap[ColumnDescriptor, Int], sorting: Seq[(ColumnDescriptor, SortBy)]): Validation[String, ProjectionDescriptor] = {
    val identities = indexedColumns.values.toSeq.sorted.foldLeft(Option(0)) {
      // test that identities are 0-based and sequential
      case (Some(cur), next) if cur == next => Some(cur + 1)
      case (Some(cur), next) if cur >  next => Some(cur)
      case _ => None
    }

    identities.toSuccess("Column identity indexes must be 0-based and must be sequential when sorted")
    .ensure("A projection may not store values of multiple types for the same selector") { _ =>   
      indexedColumns.keys.groupBy(c => (c.path, c.selector)).values.forall(_.size == 1)
    }

    .ensure("Each identity in a projection may not by shared by column descriptors which sort ById or ByValueThenId more than once") { _ =>
      def sortByIndices(indexedColumns: ListMap[ColumnDescriptor, Int], sorting: Seq[(ColumnDescriptor, SortBy)]) = {
        indexedColumns.keys.foldLeft(Map.empty: Map[Int, List[SortBy]]) {
          case (indexMap, col) => 
            val sortingMap = sorting.toMap
            if (indexMap.contains(indexedColumns(col))) indexMap + ((indexedColumns(col), indexMap(indexedColumns(col)) :+ sortingMap(col))) 
            else indexMap + ((indexedColumns(col), List(sortingMap(col))))
        }
      }

      indexedColumns.values.toSet.forall(id => 
        (sortByIndices(indexedColumns, sorting)(id).count(a => a == ById || a == ByValueThenId) < 2)
      )
    }

    .map(new ProjectionDescriptor(_, indexedColumns, sorting))
  }
}




trait ByteProjection {
  def descriptor: ProjectionDescriptor

  def project(id: Identities, v: Seq[CValue]): (Array[Byte], Array[Byte]) 
  def unproject(keyBytes: Array[Byte], valueBytes: Array[Byte]): (Identities,Seq[CValue])
  def keyOrder: Order[Array[Byte]]
}

