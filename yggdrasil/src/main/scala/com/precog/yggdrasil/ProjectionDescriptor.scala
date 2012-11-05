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

import com.precog.common.json._
import com.precog.common.json.CPath.{CPathDecomposer, CPathExtractor}
import com.precog.common._
import com.precog.util.IOUtils

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.Extractor._
import blueeyes.json.serialization.DefaultSerialization._

import akka.actor._
import akka.actor.Actor._
import akka.routing._
import akka.dispatch.Future

import java.io.File

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

case class Authorities(uids: Set[String]) {

  @tailrec
  final def hashSeq(l: Seq[String], hash: Int, i: Int = 0): Int = {
    if(i < l.length) {
      hashSeq(l, hash * 31 + l(i).hashCode, i+1)
    } else {
      hash
    }     
  }    

  lazy val hash = {
    if(uids.size == 0) 1 
    else if(uids.size == 1) uids.head.hashCode 
    else hashSeq(uids.toSeq, 1) 
  }

  override def hashCode(): Int = hash
}

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
}

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
       (obj \ "selector").validated[CPath] |@|
       (obj \ "valueType").validated[CType] |@|
       (obj \ "authorities").validated[Authorities]).apply(ColumnDescriptor(_,_,_,_))
  }
}

object ColumnDescriptor extends ColumnDescriptorSerialization with ((Path, CPath, CType, Authorities) => ColumnDescriptor) {
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
    IOUtils.safeWriteToFile(descriptor.serialize.renderPretty, path)
  }

  def fromFile(path: File): IO[Validation[Error,ProjectionDescriptor]] = {
    IOUtils.readFileToString(path).map {
      JParser.parse(_).validated[ProjectionDescriptor]
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

