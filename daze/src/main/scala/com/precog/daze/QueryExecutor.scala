package com.precog
package daze

import com.precog.yggdrasil.TableModule
import com.precog.common._

import com.precog.common.security._

import blueeyes.json._
import blueeyes.core.http.MimeType
import blueeyes.core.http.MimeTypes

import akka.util.Duration

import scalaz.{ Validation, StreamT, Id, Applicative, NonEmptyList, Semigroup }
import NonEmptyList.nels
import Validation._

sealed trait EvaluationError
case class InvalidStateError(message: String) extends EvaluationError
case class SystemError(error: Throwable) extends EvaluationError
case class AccumulatedErrors(errors: NonEmptyList[EvaluationError]) extends EvaluationError

object EvaluationError {
  def invalidState(message: String): EvaluationError = InvalidStateError(message)
  def systemError(error: Throwable): EvaluationError = SystemError(error)
  def acc(errors: NonEmptyList[EvaluationError]): EvaluationError = AccumulatedErrors(errors)

  implicit val semigroup: Semigroup[EvaluationError] = new Semigroup[EvaluationError] {
    def append(a: EvaluationError, b: => EvaluationError) = (a, b) match {
      case (AccumulatedErrors(a0), AccumulatedErrors(b0)) => AccumulatedErrors(a0 append b0)
      case (a0, AccumulatedErrors(b0)) => AccumulatedErrors(a0 <:: b0)
      case (AccumulatedErrors(a0), b0) => AccumulatedErrors(b0 <:: a0)
      case (a0, b0) => AccumulatedErrors(nels(a0, b0))
    }
  }
}

case class QueryOptions(
  page: Option[(Long, Long)] = None,
  sortOn: List[CPath] = Nil,
  sortOrder: TableModule.DesiredSortOrder = TableModule.SortAscending,
  timeout: Option[Duration] = None,
  output: MimeType = MimeTypes.application/MimeTypes.json
)

trait QueryExecutor[M[+_], +A] { self =>
  /**
    * Execute the provided query, returning the *values* of the result set (discarding identities)
    */
  def execute(query: String, context: EvaluationContext, opts: QueryOptions): M[Validation[EvaluationError, A]]

  def map[B](f: A => B)(implicit M: Applicative[M]): QueryExecutor[M, B] = new QueryExecutor[M, B] {
    import scalaz.syntax.monad._
    def execute(query: String, context: EvaluationContext, opts: QueryOptions): M[Validation[EvaluationError, B]] = {
      self.execute(query, context, opts) map { _ map f }
    }
  }
}

object NullQueryExecutor extends QueryExecutor[Id.Id, Nothing] {
  def execute(query: String, context: EvaluationContext, opts: QueryOptions) = {
    failure(SystemError(new UnsupportedOperationException("Query service not avaialble")))
  }
}
