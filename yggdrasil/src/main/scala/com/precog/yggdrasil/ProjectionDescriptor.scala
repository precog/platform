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

object Authorities {
  implicit val AuthoritiesDecomposer: Decomposer[Authorities] = new Decomposer[Authorities] {
    override def decompose(authorities: Authorities): JValue = {
      JObject(JField("uids", JArray(authorities.ownerAccountIds.map(JString(_)).toList)) :: Nil)
    }
  }

  implicit val AuthoritiesExtractor: Extractor[Authorities] = new Extractor[Authorities] {
    override def validated(obj: JValue): Validation[Error, Authorities] =
      (obj \ "uids").validated[Set[String]].map(Authorities(_))
  }

  val None = Authorities(Set())
}

/** 
 * The descriptor for a projection
 *
 * @param identities The number of identities in this projection
 */
case class ProjectionDescriptor(identities: Int, path: Path, authorities: Authorities)

object ProjectionDescriptor {
  implicit object briefShow extends Show[ProjectionDescriptor] {
    override def shows(d: ProjectionDescriptor) = {
      d.toString
    }
  }
}
