package com.precog.yggdrasil
package execution

import com.precog.yggdrasil.TableModule
import com.precog.yggdrasil.vfs._
import com.precog.yggdrasil.vfs.ResourceError._
import com.precog.common._

import com.precog.common.security._

import blueeyes.json._
import blueeyes.core.http.MimeType
import blueeyes.core.http.MimeTypes

import akka.util.Duration

import scalaz._
import scalaz.Validation._
import scalaz.NonEmptyList.nels
import scalaz.syntax.monad._

sealed trait EvaluationError
case class InvalidStateError(message: String) extends EvaluationError
case class StorageError(error: ResourceError) extends EvaluationError
case class SystemError(error: Throwable) extends EvaluationError
case class AccumulatedErrors(errors: NonEmptyList[EvaluationError]) extends EvaluationError

object EvaluationError {
  def invalidState(message: String): EvaluationError = InvalidStateError(message)
  def storageError(error: ResourceError): EvaluationError = StorageError(error)
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
  output: MimeType = MimeTypes.application/MimeTypes.json,
  cacheControl: CacheControl = CacheControl.NoCache
)

case class CacheControl(maxAge: Option[Long], recacheAfter: Option[Long], cacheable: Boolean, onlyIfCached: Boolean)

object CacheControl {
  import blueeyes.core.http.CacheDirective
  import blueeyes.core.http.CacheDirectives.{ `max-age`, `no-cache`, `only-if-cached`, `max-stale` }
  import scalaz.syntax.semigroup._
  import scalaz.std.option._
  import scalaz.std.anyVal._

  val NoCache = CacheControl(None, None, false, false)

  def fromCacheDirectives(cacheDirectives: CacheDirective*) = {
    val maxAge = cacheDirectives.collectFirst { case `max-age`(Some(n)) => n.number * 1000 }
    val maxStale = cacheDirectives.collectFirst { case `max-stale`(Some(n)) => n.number * 1000 }
    val cacheable = cacheDirectives exists { _ != `no-cache`}
    val onlyIfCached = cacheDirectives exists { _ == `only-if-cached`}
    CacheControl(maxAge |+| maxStale, maxAge, cacheable, onlyIfCached)
  }
}


trait QueryExecutor[M[+_], +A] { self =>
  def execute(apiKey: APIKey, query: String, prefix: Path, opts: QueryOptions): EitherT[M, EvaluationError, A]

  def map[B](f: A => B)(implicit M: Functor[M]): QueryExecutor[M, B] = new QueryExecutor[M, B] {
    def execute(apiKey: APIKey, query: String, prefix: Path, opts: QueryOptions): EitherT[M, EvaluationError, B] = {
      self.execute(apiKey, query, prefix, opts) map f
    }
  }
}

class NullQueryExecutor[M[+_]: Monad] extends QueryExecutor[M, Nothing] {
  def execute(apiKey: APIKey, query: String, prefix: Path, opts: QueryOptions) = {
    EitherT.left(SystemError(new UnsupportedOperationException("Query service not avaialble")).point[M])
  }
}

// vim: set ts=4 sw=4 et:
