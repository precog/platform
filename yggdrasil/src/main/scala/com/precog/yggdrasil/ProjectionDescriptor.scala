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
import scalaz.Validation._

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
case class ProjectionDescriptor(identities: Int, columns: List[ColumnDescriptor]) {
  lazy val selectors = columns.map(_.selector).toSet

  def columnAt(path: Path, selector: JPath) = columns.find(col => col.path == path && col.selector == selector)

  def satisfies(col: ColumnDescriptor) = columns.contains(col)

  private lazy val _hashCode = scala.runtime.ScalaRunTime._hashCode(ProjectionDescriptor.this)

  override def hashCode: Int = _hashCode
}

trait ProjectionDescriptorSerialization {
  implicit val ProjectionDescriptorDecomposer : Decomposer[ProjectionDescriptor] = new Decomposer[ProjectionDescriptor] {
    def decompose(pd: ProjectionDescriptor) : JValue = JObject (
      JField("columns", JArray(pd.columns.map(c => JObject(JField("descriptor", c.serialize) :: JField("index", pd.identities) :: Nil)))) :: 
      Nil
    )
  } 
  
  implicit val ProjectionDescriptorExtractor : Extractor[ProjectionDescriptor] = new Extractor[ProjectionDescriptor] with ValidatedExtraction[ProjectionDescriptor] { 
    override def validated(obj : JValue) : Validation[Error,ProjectionDescriptor] = {
      (obj \ "columns") match {
        case JArray(elements) => 
          elements.foldLeft(success[Error, (Vector[ColumnDescriptor], Int)]((Vector.empty[ColumnDescriptor], 0))) {
            case (Success((columns, identities)), obj @ JObject(fields)) =>
              val idCount = ((obj \ "index") --> classOf[JInt]).value.toInt
              (obj \ "descriptor").validated[ColumnDescriptor] map { d =>
                (columns :+ d, idCount max identities)
              }
              
            case (failure, _) => failure
          } map {
            case (columns, identities) => ProjectionDescriptor(identities, columns.toList)
          }

        case x => Failure(Invalid("Error deserializing projection descriptor: columns formatted incorrectly."))
      }
    }
  } 
}

object ProjectionDescriptor extends ProjectionDescriptorSerialization 


trait ByteProjection {
  def descriptor: ProjectionDescriptor

  def project(id: Identities, v: Seq[CValue]): (Array[Byte], Array[Byte]) 
  def unproject(keyBytes: Array[Byte], valueBytes: Array[Byte]): (Identities,Seq[CValue])
  def keyOrder: Order[Array[Byte]]
}

