package com.precog
package muspelheim

import common.Path
import common.json.CPathField
import common.kafka._

import daze._

import quirrel._
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import bytecode.JType

import yggdrasil._
import yggdrasil.actor._
import yggdrasil.serialization._
import yggdrasil.table._
import yggdrasil.util._
import muspelheim._

import org.specs2.mutable._
  
import akka.dispatch.Future
import akka.dispatch.Await
import akka.util.Duration

import java.io.File

import scalaz._
import scalaz.std.anyVal._
import scalaz.syntax.copointed._
import scalaz.effect.IO

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

import org.slf4j.LoggerFactory

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext

trait ParseEvalStackSpecs[M[+_]] extends Specification 
    with ParseEvalStack[M]
    with MemoryDatasetConsumer[M] 
    with IdSourceScannerModule { self =>

  protected lazy val parseEvalLogger = LoggerFactory.getLogger("com.precog.muspelheim.ParseEvalStackSpecs")

  val sliceSize = 10
  
  def controlTimeout = Duration(5, "minutes")      // it's just unreasonable to run tests longer than this
  
  implicit val actorSystem = ActorSystem("platformSpecsActorSystem")

  implicit def asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

  class ParseEvalStackSpecConfig extends BaseConfig with IdSourceConfig {
    parseEvalLogger.trace("Init yggConfig")
    val config = Configuration parse {
      Option(System.getProperty("precog.storage.root")) map { "precog.storage.root = " + _ } getOrElse { "" }
    }

    val sortWorkDir = scratchDir
    val memoizationBufferSize = sortBufferSize
    val memoizationWorkDir = scratchDir

    val flatMapTimeout = Duration(100, "seconds")
    val maxEvalDuration = controlTimeout
    val clock = blueeyes.util.Clock.System
    
    val maxSliceSize = self.sliceSize
    val smallSliceSize = 3

    val idSource = new FreshAtomicIdSource
  }

  def eval(str: String, debug: Boolean = false): Set[SValue] = evalE(str, debug) map { _._2 }
  
  def evalE(str: String, debug: Boolean = false): Set[SEvent] = {
    parseEvalLogger.debug("Beginning evaluation of query: " + str)
    
    val preForest = compile(str)
    val forest = preForest filter { _.errors.isEmpty }
    
    forest must haveSize(1) or {
      forall(preForest) { tree =>
        tree.errors must beEmpty
      }
    }
    
    val tree = forest.head
    
    val Right(dag) = decorate(emit(tree))
    consumeEval("dummyAPIKey", dag, Path.Root) match {
      case Success(result) =>
        parseEvalLogger.debug("Evaluation complete for query: " + str)
        result
      case Failure(error) => throw error
    }
  }
}

// vim: set ts=4 sw=4 et:
