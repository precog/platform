package com.precog.muspelheim

import com.precog.common._
import com.precog.common.security._
import com.precog.daze.QueryExecutor
import blueeyes.json._
import scalaz.Validation

trait MetadataClient[M[+_]] {
  def browse(apiKey: APIKey, path: Path): M[Validation[String, JArray]]
  def structure(apiKey: APIKey, path: Path): M[Validation[String, JObject]]
}

trait Platform[M[+_], +A] {
  def metadataClient: MetadataClient[M]
  def executorFor(apiKey: APIKey): M[Validation[String, QueryExecutor[M, A]]]
}
