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
package com.precog.bifrost

import com.precog.yggdrasil.TableModule.{ DesiredSortOrder, SortAscending, SortDescending }
import com.precog.common._

import com.precog.mimir._

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
