package com.precog
package daze

import yggdrasil.metadata.MetadataView

import com.precog.common._

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
  def browse(userUID: String, path: Path): Future[Validation[String, JArray]]
  def structure(userUID: String, path: Path): Future[Validation[String, JObject]]
  def status(): Future[Validation[String, JValue]]
  def startup(): Future[Boolean]
  def shutdown(): Future[Boolean]
}

trait NullQueryExecutor extends QueryExecutor {
  def actorSystem: ActorSystem
  implicit def executionContext: ExecutionContext

  def execute(userUID: String, query: String) = {
    failure(SystemError(new UnsupportedOperationException("Query service not avaialble")))
  }
  
  def browse(userUID: String, path: Path) = sys.error("feature not available") 
  def structure(userUID: String, path: Path) = sys.error("feature not available")
  def status() = sys.error("feature not available")

  def startup = Future(true)
  def shutdown = Future { actorSystem.shutdown; true }
}

// vim: set ts=4 sw=4 et:
