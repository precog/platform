package com.precog.yggdrasil

import com.precog.common._
import com.precog.common.json._
import com.precog.common.accounts._
import com.precog.common.security.Authorities
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

//sealed trait SortBy
//case object ById extends SortBy
//case object ByValue extends SortBy
//case object ByValueThenId extends SortBy
//
//trait SortBySerialization {
//  implicit val SortByDecomposer : Decomposer[SortBy] = new Decomposer[SortBy] {
//    def decompose(sortBy: SortBy) : JValue = JString(toName(sortBy)) 
//  }
//
//  implicit val SortByExtractor : Extractor[SortBy] = new Extractor[SortBy] {
//    override def validated(obj : JValue) : Validation[Error,SortBy] = obj match {
//      case JString(s) => fromName(s).map(Success(_)).getOrElse(Failure(Invalid("Unknown SortBy property: " + s))) 
//      case _          => Failure(Invalid("Expected JString type for SortBy property"))
//    }
//  }
//
//  def fromName(s: String): Option[SortBy] = s match {
//    case "ById"          => Some(ById)
//    case "ByValue"       => Some(ByValue)
//    case "ByValueThenId" => Some(ByValueThenId)
//    case _               => None
//  }
//
//  def toName(sortBy: SortBy): String = sortBy match {
//    case ById => "ById"
//    case ByValue => "ByValue"
//    case ByValueThenId => "ByValueThenId"
//  }
//}
//
//object SortBy extends SortBySerialization
//
//
///** 
// * The descriptor for a projection
// *
// * @param identities The number of identities in this projection
// */
//case class ProjectionDescriptor(identities: Int, path: Path, authorities: Authorities)
//
//object ProjectionDescriptor {
//  implicit object briefShow extends Show[ProjectionDescriptor] {
//    override def shows(d: ProjectionDescriptor) = {
//      d.toString
//    }
//  }
//}
