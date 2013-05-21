package com.precog.shard

import com.precog.common._
import com.precog.common.security.APIKey

import com.precog.yggdrasil._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.execution._

import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._

import scalaz._
import scalaz.Validation._
import scalaz.std.anyVal._
import scalaz.std.iterable._
import scalaz.syntax.monad._
import scalaz.syntax.foldable._

/*
class StorageMetadataClient[M[+_]: Monad](metadata: StorageMetadataSource[M]) extends MetadataClient[M] {
  def currentVersion(apiKey: APIKey, path: Path) = metadata.userMetadataView(apiKey).currentVersion(path)

  def currentAuthorities(apiKey: APIKey, path: Path) = metadata.userMetadataView(apiKey).currentAuthorities(path)
}
*/
