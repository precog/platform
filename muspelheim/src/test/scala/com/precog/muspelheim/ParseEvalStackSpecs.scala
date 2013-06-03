package com.precog
package muspelheim

import common._
import common.kafka._

import com.precog.util._
import com.precog.common.accounts._

import daze._

import quirrel._
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import bytecode.JType

import yggdrasil._
import yggdrasil.actor._
import com.precog.yggdrasil.execution.EvaluationContext
import yggdrasil.serialization._
import yggdrasil.table._
import yggdrasil.util._
import muspelheim._

import org.specs2.mutable._

import org.joda.time.DateTime

import akka.dispatch.Future
import akka.dispatch.Await
import akka.util.Duration

import java.io.File

import scalaz._
import scalaz.std.anyVal._
import scalaz.effect.IO

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

import org.slf4j.LoggerFactory

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext

trait ParseEvalStackSpecs[M[+_]] extends Specification {
  type TestStack <: TestStackLike[M]
}

object TestStack {
  val testAPIKey = "dummyAPIKey"
  val testAccount = "dummyAccount"
}

trait ActorPlatformSpecs {
  implicit val actorSystem = ActorSystem("platformSpecsActorSystem")
  implicit val executor = ExecutionContext.defaultExecutionContext(actorSystem)
}

trait TestStackLike[M[+_]] extends ParseEvalStack[M]
    with EchoHttpClientModule[M]
    with MemoryDatasetConsumer[M]
    with IdSourceScannerModule 
    with EvalStackLike { self =>
  import TestStack._

  protected lazy val parseEvalLogger = LoggerFactory.getLogger("com.precog.muspelheim.ParseEvalStackSpecs")

  class ParseEvalStackSpecConfig extends BaseConfig with IdSourceConfig {
    parseEvalLogger.trace("Init yggConfig")
    val config = Configuration parse {
      Option(System.getProperty("precog.storage.root")) map { "precog.storage.root = " + _ } getOrElse { "" }
    }

    val sortWorkDir = scratchDir
    val memoizationBufferSize = sortBufferSize
    val memoizationWorkDir = scratchDir

    val flatMapTimeout = Duration(100, "seconds")
    val maxEvalDuration = Duration(5, "minutes")      // it's just unreasonable to run tests longer than this
    val clock = blueeyes.util.Clock.System

    val maxSliceSize = 10
    val smallSliceSize = 3

    val idSource = new FreshAtomicIdSource
  }

  private val dummyAccount = AccountDetails("dummyAccount", "nobody@precog.com",
    new DateTime, "dummyAPIKey", Path.Root, AccountPlan.Free)
  private def dummyEvaluationContext = EvaluationContext("dummyAPIKey", dummyAccount, Path.Root, new DateTime)

  def eval(str: String, debug: Boolean = false): Set[SValue] = evalE(str, debug) map { _._2 }

  def evalE(str: String, debug: Boolean = false): Set[SEvent] = {
    parseEvalLogger.debug("Beginning evaluation of query: " + str)

    val preForest = compile(str)
    val forest = preForest filter { _.errors filterNot isWarning isEmpty }

    assert(forest.size == 1 || preForest.forall(_.errors filterNot isWarning isEmpty))
    val tree = forest.head

    val Right(dag) = decorate(emit(tree))
    consumeEval(dag, dummyEvaluationContext) match {
      case Success(result) =>
        parseEvalLogger.debug("Evaluation complete for query: " + str)
        result
      case Failure(error) => throw error
    }
  }
}
