package com.precog.yggdrasil

import com.precog.common.json._
import com.precog.common.json.CPath.{CPathDecomposer, CPathExtractor}
import com.precog.common._
import com.precog.common.accounts._
import com.precog.util.IOUtils

import com.google.common.base.Charsets
import com.google.common.hash.Hashing

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.Extractor._
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.DefaultSerialization._

import akka.actor._
import akka.actor.Actor._
import akka.routing._
import akka.dispatch.Future

import java.io.File

import shapeless._
import scalaz._
import scalaz.Validation._
import scalaz.effect.IO
import scalaz.syntax.apply._
import scalaz.syntax.bifunctor._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scala.annotation.tailrec
import scala.collection.immutable.ListMap

sealed trait SortBy
case object ById extends SortBy
case object ByValue extends SortBy
case object ByValueThenId extends SortBy

trait SortBySerialization {
  implicit val SortByDecomposer : Decomposer[SortBy] = new Decomposer[SortBy] {
    def decompose(sortBy: SortBy) : JValue = JString(toName(sortBy)) 
  }

  implicit val SortByExtractor : Extractor[SortBy] = new Extractor[SortBy] {
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

case class Authorities(ownerAccountIds: Set[AccountId]) {

  @tailrec
  final def hashSeq(l: Seq[String], hash: Int, i: Int = 0): Int = {
    if(i < l.length) {
      hashSeq(l, hash * 31 + l(i).hashCode, i+1)
    } else {
      hash
    }     
  }    

  lazy val hash = {
    if(ownerAccountIds.size == 0) 1 
    else if(ownerAccountIds.size == 1) ownerAccountIds.head.hashCode 
    else hashSeq(ownerAccountIds.toSeq, 1) 
  }

  override def hashCode(): Int = hash
}

trait AuthoritiesSerialization {
  implicit val AuthoritiesDecomposer: Decomposer[Authorities] = new Decomposer[Authorities] {
    override def decompose(authorities: Authorities): JValue = {
      JObject(JField("uids", JArray(authorities.ownerAccountIds.map(JString(_)).toList)) :: Nil)
    }
  }

  implicit val AuthoritiesExtractor: Extractor[Authorities] = new Extractor[Authorities] {
    override def validated(obj: JValue): Validation[Error, Authorities] =
      (obj \ "uids").validated[Set[String]].map(Authorities(_))
  }
}

object Authorities extends AuthoritiesSerialization {
  val None = Authorities(Set())
}

case class ColumnDescriptor(path: Path, selector: CPath, valueType: CType, authorities: Authorities) {
  lazy val hash = {
    var hash = 1
    hash = hash * 31 + path.hashCode
    hash = hash * 31 + selector.path.hashCode
    hash = hash * 31 + valueType.hashCode
    hash * 31 + authorities.hashCode
  }

  override def hashCode(): Int = hash
  
  override def equals(other: Any): Boolean = other match {
    case o @ ColumnDescriptor(p, s, vt, a) =>
      path == p && selector == s && valueType == vt && authorities == a 
    case _ => false
  }

  def isChildOf(parentPath: Path, parentSelector: CPath): Boolean = 
    path == parentPath && (selector.nodes startsWith parentSelector.nodes)

  def isChildOf(parentPath: Path): Boolean =
    path isChildOf parentPath
}

object ColumnDescriptor extends ((Path, CPath, CType, Authorities) => ColumnDescriptor) {
  implicit val iso = Iso.hlist(ColumnDescriptor.apply _, ColumnDescriptor.unapply _)
  val schemaV1 = "path" :: "selector" :: "valueType" :: "authorities" :: HNil
  implicit val (decomposer, extractor) = IsoSerialization.serialization[ColumnDescriptor](schemaV1)

  implicit object briefShow extends Show[ColumnDescriptor] {
    override def shows(d: ColumnDescriptor) = {
      "%s::%s (%s)".format(d.path.path, d.selector.toString, d.valueType.toString)
    }
  }
}

/** 
 * The descriptor for a projection
 *
 * @param identities The number of identities in this projection
 * @param columns The descriptors for all non-identity columns
 */
case class ProjectionDescriptor(identities: Int, columns: List[ColumnDescriptor]) {
  lazy val selectors = columns.map(_.selector).toSet

  def columnAt(path: Path, selector: CPath) = columns.find(col => col.path == path && col.selector == selector)

  def satisfies(col: ColumnDescriptor) = columns.contains(col)

  def commonPrefix: List[String] = {
    val paths = columns.map(_.path.components)
    paths.tail.foldLeft(paths.head) {
      case (prefix, nextPath) => (prefix zip nextPath).takeWhile { case(p1, p2) => p1 == p2 }.unzip._1
    }
  }

  def stableHash: String = Hashing.sha256().hashString(this.toString, Charsets.UTF_8).toString
}

trait ProjectionDescriptorSerialization {
  implicit val ProjectionDescriptorDecomposer : Decomposer[ProjectionDescriptor] = new Decomposer[ProjectionDescriptor] {
    def decompose(pd: ProjectionDescriptor) : JValue = JObject (
      JField("columns", JArray(pd.columns.map(c => JObject(JField("descriptor", c.jv) :: JField("index", pd.identities.jv) :: Nil)))) :: 
      Nil
    )
  } 
  
  implicit val ProjectionDescriptorExtractor : Extractor[ProjectionDescriptor] = new Extractor[ProjectionDescriptor] { 
    override def validated(obj : JValue) : Validation[Error,ProjectionDescriptor] = {
      (obj \ "columns") match {
        case JArray(elements) => 
          elements.foldLeft(success[Error, (Vector[ColumnDescriptor], Int)]((Vector.empty[ColumnDescriptor], 0))) {
            case (Success((columns, identities)), obj @ JObject(fields)) =>
              val idCount = ((obj \ "index") --> classOf[JNum]).toLong.toInt
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

  def toFile(descriptor: ProjectionDescriptor, path: File): IO[Boolean] = {
    IOUtils.safeWriteToFile(descriptor.jv.renderPretty, path)
  }

  def fromFile(path: File): IO[Validation[Error,ProjectionDescriptor]] = {
    IOUtils.readFileToString(path).map {
      JParser.parseUnsafe(_).validated[ProjectionDescriptor]
    }
  }

  implicit object briefShow extends Show[ProjectionDescriptor] {
    override def shows(d: ProjectionDescriptor) = {
      d.columns.map(c => c.shows).mkString("[", ", ", "]")
    }
  }
}

object ProjectionDescriptor extends ProjectionDescriptorSerialization 


trait ByteProjection {
  def descriptor: ProjectionDescriptor

  def toBytes(id: Identities, v: Seq[CValue]): (Array[Byte], Array[Byte]) 
  def fromBytes(keyBytes: Array[Byte], valueBytes: Array[Byte]): (Identities,Seq[CValue])
  def keyByteOrder: Order[Array[Byte]]
}

