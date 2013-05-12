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
