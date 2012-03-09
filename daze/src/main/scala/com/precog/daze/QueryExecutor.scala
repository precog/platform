package com.precog
package daze

import blueeyes.json.JsonAST._

import akka.actor.ActorSystem
import akka.dispatch.{Future, ExecutionContext}

import scalaz.Validation
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

trait QueryExecutor {
  def execute(userUID: String, query: String): Validation[EvaluationError, JArray]
  def startup: Future[Unit]
  def shutdown: Future[Unit]
}

trait NullQueryExecutor extends QueryExecutor {
  def actorSystem: ActorSystem
  implicit def executionContext: ExecutionContext

  def execute(userUID: String, query: String) = {
    failure(SystemError(new UnsupportedOperationException("Query service not avaialble")))
  }

  def startup = Future(())
  def shutdown = Future { actorSystem.shutdown }
}

// vim: set ts=4 sw=4 et:
