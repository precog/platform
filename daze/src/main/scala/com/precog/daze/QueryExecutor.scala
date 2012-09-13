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

import com.precog.common._

import blueeyes.json.JsonAST._

import akka.actor.ActorSystem
import akka.dispatch.{Future, ExecutionContext}

import scalaz.{ Validation, StreamT, Id }
import Validation._

sealed trait EvaluationError
case class UserError(errorData: JArray) extends EvaluationError
case class AccessDenied(reason: String) extends EvaluationError
case object TimeoutError extends EvaluationError
case class SystemError(error: Throwable) extends EvaluationError

object EvaluationError {
  def userError(errorData: JArray):  EvaluationError = UserError(errorData)
  def systemError(error: Throwable): EvaluationError = SystemError(error)
  val timeoutError: EvaluationError = TimeoutError
}

trait QueryExecutor[M[+_]] {
  def execute(userUID: String, query: String, prefix: Path): Validation[EvaluationError, StreamT[M, List[JValue]]]
  def browse(userUID: String, path: Path): M[Validation[String, JArray]]
  def structure(userUID: String, path: Path): M[Validation[String, JObject]]
  def status(): M[Validation[String, JValue]]
  def startup(): M[Boolean]
  def shutdown(): M[Boolean]
}

trait NullQueryExecutor extends QueryExecutor[Id.Id] {
  def actorSystem: ActorSystem
  implicit def executionContext: ExecutionContext

  def execute(userUID: String, query: String, prefix: Path) = {
    failure(SystemError(new UnsupportedOperationException("Query service not avaialble")))
  }
  
  def browse(userUID: String, path: Path) = sys.error("feature not available") 
  def structure(userUID: String, path: Path) = sys.error("feature not available")
  def status() = sys.error("feature not available")

  def startup = true
  def shutdown = true
}

// vim: set ts=4 sw=4 et:
