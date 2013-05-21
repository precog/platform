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

import com.precog.common.security._

import blueeyes.json._

import akka.util.Duration

import java.nio.CharBuffer

import scalaz.{ Validation, StreamT, Id, Applicative }
import Validation._

sealed trait EvaluationError
case class InvalidStateError(message: String) extends EvaluationError
case class SystemError(error: Throwable) extends EvaluationError

object EvaluationError {
  def systemError(error: Throwable): EvaluationError = SystemError(error)
}

sealed trait QueryOutput
case object JSONOutput extends QueryOutput
case object CSVOutput extends QueryOutput

case class QueryOptions(
  page: Option[(Long, Long)] = None,
  sortOn: List[CPath] = Nil,
  sortOrder: TableModule.DesiredSortOrder = TableModule.SortAscending,
  timeout: Option[Duration] = None,
  output: QueryOutput = JSONOutput
)

trait QueryExecutor[M[+_], +A] { self =>
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
