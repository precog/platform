package com.precog
package daze

import com.precog.yggdrasil.TableModule
import com.precog.common._
import com.precog.common.json._
import com.precog.common.security._

import blueeyes.json._

import akka.actor.ActorSystem
import akka.dispatch.{Future, ExecutionContext}

import java.nio.CharBuffer

import scalaz.{ Validation, StreamT, Id }
import Validation._

sealed trait EvaluationError
case class UserError(errorData: JArray) extends EvaluationError
case class InvalidStateError(message: String) extends EvaluationError
case class AccessDenied(reason: String) extends EvaluationError
case object TimeoutError extends EvaluationError
case class SystemError(error: Throwable) extends EvaluationError

object EvaluationError {
  def userError(errorData: JArray):  EvaluationError = UserError(errorData)
  def systemError(error: Throwable): EvaluationError = SystemError(error)
  val timeoutError: EvaluationError = TimeoutError
}

case class QueryOptions(
  page: Option[(Long, Long)] = None,
  sortOn: List[CPath] = Nil,
  sortOrder: TableModule.DesiredSortOrder = TableModule.SortAscending,
  timeout: Option[Long] = None)

trait MetadataClient[M[+_]] {
  def browse(apiKey: APIKey, path: Path): M[Validation[String, JArray]]
  def structure(apiKey: APIKey, path: Path): M[Validation[String, JObject]]
}

trait QueryExecutor[M[+_]] {
  def execute(apiKey: APIKey, query: String, prefix: Path, opts: QueryOptions): M[Validation[EvaluationError, StreamT[M, CharBuffer]]]
}

trait QueryExecutorFactory[M[+_]] extends MetadataClient[M] {
  def executorFor(apiKey: APIKey): M[Validation[String, QueryExecutor[M]]]
  def status(): M[Validation[String, JValue]]
  def startup(): M[Boolean]
  def shutdown(): M[Boolean]
}

trait NullQueryExecutor extends QueryExecutor[Id.Id] {
  def actorSystem: ActorSystem
  implicit def executionContext: ExecutionContext

  def execute(apiKey: APIKey, query: String, prefix: Path, opts: QueryOptions) = {
    failure(SystemError(new UnsupportedOperationException("Query service not avaialble")))
  }
  
  def browse(apiKey: APIKey, path: Path) = sys.error("feature not available") 
  def structure(apiKey: APIKey, path: Path) = sys.error("feature not available")
  def status() = sys.error("feature not available")

  def startup = true
  def shutdown = true
}

// vim: set ts=4 sw=4 et:
