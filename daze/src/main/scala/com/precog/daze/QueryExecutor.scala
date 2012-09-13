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
