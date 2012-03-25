package com.precog
package daze

import yggdrasil._

import akka.dispatch.Await
import scalaz.Validation
import scalaz.effect.IO
import scalaz.iteratee._
import scalaz.syntax.monad._
import scalaz.std.set._
import Validation._
import Iteratee._

trait DatasetConsumersConfig extends EvaluatorConfig {
  def maxEvalDuration: akka.util.Duration
}

// TODO decouple this from the evaluator specifics
trait MemoryDatasetConsumer extends Evaluator with YggConfigComponent {
  type X = Throwable
  type Dataset[E] <: IterableDataset[E]
  type YggConfig <: DatasetConsumersConfig 

  def error(msg: String, ex: Throwable): X = new RuntimeException(msg, ex)

  def consumeEval(userUID: String, graph: DepGraph): Validation[X, Set[SEvent]] = {
    implicit val bind = Validation.validationMonad[Throwable]
    //val validated: Validation[X, Validation[X, Set[SEvent]]] = Validation.fromTryCatch {
      success(eval(userUID, graph).iterable.toSet)
    //} 
    
    //validated.fail.map(err => error("Timed out after " + yggConfig.maxEvalDuration + " in consumeEval", err)).validation.join
  }
}


// vim: set ts=4 sw=4 et:
