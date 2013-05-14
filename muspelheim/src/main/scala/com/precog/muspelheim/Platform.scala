package com.precog.muspelheim

import com.precog.common._

import com.precog.common.security._
import com.precog.daze.QueryExecutor
import com.precog.yggdrasil.vfs.VersionEntry
import blueeyes.json._
import scalaz.Validation

trait MetadataClient[M[+_]] {
  def size(userUID: String, path: Path): M[Validation[String, JNum]]
  def browse(apiKey: APIKey, path: Path): M[Validation[String, JArray]]
  def structure(apiKey: APIKey, path: Path, property: CPath): M[Validation[String, JObject]]
  def currentVersion(apiKey: APIKey, path: Path): M[Option[VersionEntry]]
  def currentAuthorities(apiKey: APIKey, path: Path): M[Option[Authorities]]
}

trait Platform[M[+_], +A] {
  def metadataClient: MetadataClient[M]
  def executorFor(apiKey: APIKey): M[Validation[String, QueryExecutor[M, A]]]
}

