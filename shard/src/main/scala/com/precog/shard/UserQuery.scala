package com.precog.shard

import com.precog.yggdrasil.TableModule.{ DesiredSortOrder, SortAscending, SortDescending }
import com.precog.common._
import com.precog.common.json._
import com.precog.daze._

import blueeyes.json.{ serialization => _, _ }
import blueeyes.json.serialization.{ Decomposer, Extractor, ValidatedExtraction }
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeExtractor => _, DateTimeDecomposer => _, _ }

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

    implicit val SortOrderExtractor = new Extractor[DesiredSortOrder] with ValidatedExtraction[DesiredSortOrder] {
      def decompose(obj: JValue): Validation[Extractor.Error, DesiredSortOrder] = obj match {
        case JString("asc") => Success(SortAscending)
        case JString("desc") => Success(SortDescending)
        case _ => Failure(Extractor.Invalid("Sort order can only be either 'asc' and 'desc'."))
      }
    }

    val schema = "query" :: "prefix" :: "sortOn" :: "sortOrder" :: HNil
    implicit val (queryDecomposer, queryExtractor) = serialization[UserQuery](schema)
  }
}
