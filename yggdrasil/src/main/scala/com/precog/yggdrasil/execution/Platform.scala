package com.precog.yggdrasil
package execution

import com.precog.common._

import com.precog.common.security._
import com.precog.yggdrasil.vfs.VersionEntry
import blueeyes.json._
import scalaz._

trait MetadataClient[M[+_]] {
  def size(apiKey: APIKey, path: Path): M[Validation[String, JNum]]
  def browse(apiKey: APIKey, path: Path): M[Validation[String, JArray]]
  def structure(apiKey: APIKey, path: Path, property: CPath): M[Validation[String, JObject]]
  def currentVersion(apiKey: APIKey, path: Path): M[Option[VersionEntry]]
  def currentAuthorities(apiKey: APIKey, path: Path): M[Option[Authorities]]
}

trait Platform[M[+_], +A] {
  def metadataClient: MetadataClient[M]
  def executorFor(apiKey: APIKey): EitherT[M, String, QueryExecutor[M, A]]
}

