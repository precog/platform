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
  type YggConfig <: DatasetConsumersConfig 

  def consumeEval(userUID: String, graph: DepGraph): Validation[Throwable, Set[SEvent]] = {
    implicit val bind = Validation.validationMonad[Throwable]
    val validated: Validation[Throwable, Validation[Throwable, Set[SEvent]]] = Validation.fromTryCatch {
      Await.result(
        eval(userUID, graph).fenum.map { (enum: EnumeratorP[X, Vector[SEvent], IO]) => 
          (consume[X, Vector[SEvent], IO, Set] &= enum[IO]) map { s => success[Throwable, Set[SEvent]](s.flatten) } run { err => IO(failure(err)) } unsafePerformIO
        },
        yggConfig.maxEvalDuration
      )
    } 
    
    validated.fail.map(err => new RuntimeException("Timed out after " + yggConfig.maxEvalDuration + " in consumeEval", err): Throwable).validation.join
  }
}


// vim: set ts=4 sw=4 et:
