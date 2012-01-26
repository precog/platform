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

case class ColumnDescriptor(path: Path, selector: JPath, valueType: ColumnType) 

trait ColumnDescriptorSerialization {
  implicit val ColumnDescriptorDecomposer : Decomposer[ColumnDescriptor] = new Decomposer[ColumnDescriptor] {
    def decompose(selector : ColumnDescriptor) : JValue = JObject (
      List(JField("path", selector.path.serialize),
           JField("selector", selector.selector.serialize),
           JField("valueType", selector.valueType.serialize))
    )
  }

  implicit val ColumnDescriptorExtractor : Extractor[ColumnDescriptor] = new Extractor[ColumnDescriptor] with ValidatedExtraction[ColumnDescriptor] {
    override def validated(obj : JValue) : Validation[Error,ColumnDescriptor] = 
      ((obj \ "path").validated[Path] |@|
       (obj \ "selector").validated[JPath] |@|
       (obj \ "valueType").validated[ColumnType]).apply(ColumnDescriptor(_,_,_))
  }
}

object ColumnDescriptor extends ColumnDescriptorSerialization 
with ((Path, JPath, ColumnType) => ColumnDescriptor)

/** 
 * The descriptor for a projection 
 */
case class ProjectionDescriptor private (identities: Int, indexedColumns: ListMap[ColumnDescriptor, Int], sorting: Seq[(ColumnDescriptor, SortBy)]) {
  lazy val columns = indexedColumns.map(_._1).toList 
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
  def apply(columns: ListMap[ColumnDescriptor, Int], sorting: Seq[(ColumnDescriptor, SortBy)]): Validation[String, ProjectionDescriptor] = {
    val identities = columns.values.toSeq.sorted.foldLeft(Option(0)) {
      // test that identities are 0-based and sequential
      case (Some(cur), next) if cur == next => Some(cur + 1)
      case (Some(cur), next) if cur >  next => Some(cur)
      case _ => None
    }

    identities.toSuccess("Column identity indexes must be 0-based and must be sequential when sorted")
    .map(new ProjectionDescriptor(_, columns, sorting))
  }
}

trait ByteProjection {
  def descriptor: ProjectionDescriptor

  def project(id: Identities, v: Seq[CValue]): (Array[Byte], Array[Byte]) 
  def unproject[E](keyBytes: Array[Byte], valueBytes: Array[Byte])(f: (Identities, Seq[CValue]) => E): E
}

