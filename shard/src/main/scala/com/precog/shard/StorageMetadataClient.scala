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
package com.precog.shard

import com.precog.common._
import com.precog.common.json._
import com.precog.yggdrasil._
import com.precog.yggdrasil.metadata._
import com.precog.muspelheim._

import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._

import scalaz._
import scalaz.Validation._
import scalaz.std.anyVal._
import scalaz.std.iterable._
import scalaz.syntax.monad._
import scalaz.syntax.foldable._

class StorageMetadataClient[M[+_]: Monad](metadata: StorageMetadataSource[M]) extends MetadataClient[M] {
  def size(userUID: String, path: Path): M[Validation[String, JNum]] =
    metadata.userMetadataView(userUID).findSize(path) map { s => success(JNum(s)) }

  def browse(userUID: String, path: Path): M[Validation[String, JArray]] = {
    metadata.userMetadataView(userUID).findDirectChildren(path) map {
      case paths => success(JArray(paths.map( p => JString(p.toString)).toSeq: _*))
    }
  }

  def structure(userUID: String, path: Path, property: CPath): M[Validation[String, JObject]] = {
    metadata.userMetadataView(userUID).findStructure(path, property) map {
      case PathStructure(types, children) =>
        success(JObject(Map("children" -> children.serialize,
                            "structure" -> JObject(Map("types" -> types.serialize)))))
    }
  }

//    val futRoot = metadata.userMetadataView(userUID).findPathMetadata(path, property)
//
//    def transform(children: Set[PathMetadata]): JObject = {
//      val (childNames, types) = children.foldLeft((Set.empty[String], Map.empty[String, Long])) {
//        case ((cs, ts), PathIndex(i, _)) =>
//          val path = "[%d]".format(i)
//          (cs + path, ts)
//
//        case ((cs, ts), PathField(f, _)) =>
//          val path = "." + f
//          (cs + path, ts)
//      
//        case ((cs, ts), PathValue(t, _, descriptors)) => 
//          val tname = CType.nameOf(t)
//          val counts = for {
//            colMetadata <- descriptors.values
//            (colDesc, metadata) <- colMetadata if colDesc.valueType == t
//            stats <- metadata.values collect { case s: MetadataStats => s }
//          } yield stats.count
//
//          (cs, ts + (tname -> (ts.getOrElse(tname, 0L) + counts.suml)))
//      }
//
//      JObject("children" -> childNames.serialize, "types" -> types.serialize)
//    }
//
//    futRoot.map { pr => Success(transform(pr.children)) }
//  }
}

// vim: set ts=4 sw=4 et:
