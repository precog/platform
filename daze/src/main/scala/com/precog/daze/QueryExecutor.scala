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

import scalaz.{ Validation, StreamT, Id, Applicative }
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

trait QueryExecutor[M[+_], +A] { self =>
  def execute(apiKey: APIKey, query: String, prefix: Path, opts: QueryOptions): M[Validation[EvaluationError, A]]

  def map[B](f: A => B)(implicit M: Applicative[M]): QueryExecutor[M, B] = new QueryExecutor[M, B] {
    import scalaz.syntax.monad._
    def execute(apiKey: APIKey, query: String, prefix: Path, opts: QueryOptions): M[Validation[EvaluationError, B]] = {
      self.execute(apiKey, query, prefix, opts) map { _ map f }
    }
  }
}

object NullQueryExecutor extends QueryExecutor[Id.Id, Nothing] {
  def execute(apiKey: APIKey, query: String, prefix: Path, opts: QueryOptions) = {
    failure(SystemError(new UnsupportedOperationException("Query service not avaialble")))
  }
}

// vim: set ts=4 sw=4 et:
