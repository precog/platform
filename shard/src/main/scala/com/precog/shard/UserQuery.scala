package com.precog.shard

import com.precog.yggdrasil.TableModule.{ DesiredSortOrder, SortAscending, SortDescending }
import com.precog.common._

import com.precog.daze._

import blueeyes.json._
import blueeyes.json.serialization.{ Decomposer, Extractor }
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.IsoSerialization.{serialization => isoSerialization}

import shapeless._

import scalaz._

case class UserQuery(query: String, prefix: Path, sortOn: List[CPath], sortOrder: DesiredSortOrder)

object UserQuery {
  implicit val queryIso = Iso.hlist(UserQuery.apply _, UserQuery.unapply _)

  object Serialization {
    implicit val SortOrderDecomposer = new Decomposer[DesiredSortOrder] {
      def decompose(sortOrder: DesiredSortOrder): JValue = sortOrder match {
        case SortAscending => JString("asc")
        case SortDescending => JString("desc")
      }
    }

    implicit val SortOrderExtractor = new Extractor[DesiredSortOrder] {
      def validated(obj: JValue): Validation[Extractor.Error, DesiredSortOrder] = obj match {
        case JString("asc") => Success(SortAscending)
        case JString("desc") => Success(SortDescending)
        case _ => Failure(Extractor.Invalid("Sort order can only be either 'asc' and 'desc'."))
      }
    }

    val schema = "query" :: "prefix" :: "sortOn" :: "sortOrder" :: HNil
    implicit val (queryDecomposer, queryExtractor) = isoSerialization[UserQuery](schema)
  }
}
