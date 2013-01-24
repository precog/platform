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

trait MetadataClient[M[+_]] {
  def browse(apiKey: APIKey, path: Path): M[Validation[String, JArray]]
  def structure(apiKey: APIKey, path: Path): M[Validation[String, JObject]]
}

trait QueryExecutor[M[+_], +A] extends MetadataClient[M] { self =>
  def execute(apiKey: APIKey, query: String, prefix: Path, opts: QueryOptions): M[Validation[EvaluationError, A]]

  def map[B](f: A => B)(implicit M: Applicative[M]): QueryExecutor[M, B] = new QueryExecutor[M, B] {
    import scalaz.syntax.monad._
    def execute(apiKey: APIKey, query: String, prefix: Path, opts: QueryOptions): M[Validation[EvaluationError, B]] = {
      self.execute(apiKey, query, prefix, opts) map { _ map f }
    }

    def browse(apiKey: APIKey, path: Path): M[Validation[String, JArray]] = self.browse(apiKey, path)
    def structure(apiKey: APIKey, path: Path): M[Validation[String, JObject]] = self.structure(apiKey, path)
  }
}

trait QueryExecutorFactory[M[+_], +A] {
  def executorFor(apiKey: APIKey): M[Validation[String, QueryExecutor[M, A]]]
}

object NullQueryExecutor extends QueryExecutor[Id.Id, Nothing] {
  def execute(apiKey: APIKey, query: String, prefix: Path, opts: QueryOptions) = {
    failure(SystemError(new UnsupportedOperationException("Query service not avaialble")))
  }
  
  def browse(apiKey: APIKey, path: Path) = sys.error("feature not available") 
  def structure(apiKey: APIKey, path: Path) = sys.error("feature not available")
  def status() = sys.error("feature not available")
}

// vim: set ts=4 sw=4 et:
