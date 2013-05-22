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
import com.precog.common.security.APIKey

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
  def size(apiKey: APIKey, path: Path): M[Validation[String, JNum]] =
    metadata.userMetadataView(apiKey).findSize(path) map { s => success(JNum(s)) }

  def browse(apiKey: APIKey, path: Path): M[Validation[String, JArray]] = {
    metadata.userMetadataView(apiKey).findDirectChildren(path) map {
      case paths =>
        success(JArray(paths.map { p =>
          JString(p.toString.substring(1))
        }.toSeq: _*))
    }
  }

  /**
   * This turns a set of types/counts into something usable by strucutre. It
   * will serialize the longs to JNums and unify CNumericTypes under "Number".
   */
  private def normalizeTypes(xs: Map[CType, Long]): Map[String, JValue] = {
    xs.foldLeft(Map.empty[String, Long]) {
      case (acc, ((CLong | CDouble | CNum), count)) =>
        acc + ("Number" -> (acc.getOrElse("Number", 0L) + count))
      case (acc, (ctype, count)) =>
        acc + (CType.nameOf(ctype) -> count)
    } mapValues (_.serialize)
  }

  def structure(apiKey: APIKey, path: Path, property: CPath): M[Validation[String, JObject]] = {
    metadata.userMetadataView(apiKey).findStructure(path, property) map {
      case PathStructure(types, children) =>
        success(JObject(Map("children" -> children.serialize,
                            "types" -> JObject(normalizeTypes(types)))))
    }
  }

  def currentVersion(apiKey: APIKey, path: Path) = metadata.userMetadataView(apiKey).currentVersion(path)

  def currentAuthorities(apiKey: APIKey, path: Path) = metadata.userMetadataView(apiKey).currentAuthorities(path)
}
