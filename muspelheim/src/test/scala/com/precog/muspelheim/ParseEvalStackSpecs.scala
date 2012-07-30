package com.precog
package muspelheim

import common.VectorCase
import common.kafka._

import daze._
import daze.util._

import quirrel._
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import bytecode.JType

import yggdrasil._
import yggdrasil.actor._
import yggdrasil.memoization._
import yggdrasil.serialization._
import yggdrasil.table._
import muspelheim._

import org.specs2.mutable._
  
import akka.dispatch.Future
import akka.dispatch.Await
import akka.util.Duration

import java.io.File

import scalaz._
import scalaz.effect.IO

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

import com.weiglewilczek.slf4s.Logging

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext

trait ParseEvalStackSpecs[M[+_]] extends Specification 
    with ParseEvalStack[M]
    with StorageModule[M]
    with MemoryDatasetConsumer[M] 
    with IdSourceScannerModule[M]
    with Logging {

  val sliceSize = 10
  
  def controlTimeout = Duration(30, "seconds")      // it's just unreasonable to run tests longer than this
  
  implicit val actorSystem = ActorSystem("platformSpecsActorSystem")

  implicit def asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

  class ParseEvalStackSpecConfig extends BaseConfig with DatasetConsumersConfig {
    logger.trace("Init yggConfig")
    val config = Configuration parse {
      Option(System.getProperty("precog.storage.root")) map { "precog.storage.root = " + _ } getOrElse { "" }
    }

    val sortWorkDir = scratchDir
    val memoizationBufferSize = sortBufferSize
    val memoizationWorkDir = scratchDir

    val flatMapTimeout = Duration(100, "seconds")
    val projectionRetrievalTimeout = akka.util.Timeout(Duration(10, "seconds"))
    val maxEvalDuration = controlTimeout
    val clock = blueeyes.util.Clock.System

    object valueSerialization extends SortSerialization[SValue] with SValueRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization
    object eventSerialization extends SortSerialization[SEvent] with SEventRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization
    object groupSerialization extends SortSerialization[(SValue, Identities, SValue)] with GroupRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization
    object memoSerialization extends IncrementalSerialization[(Identities, SValue)] with SEventRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization

    //TODO: Get a producer ID
    val idSource = new IdSource {
      private val source = new java.util.concurrent.atomic.AtomicLong
      def nextId() = source.getAndIncrement
    }
  }

  step {
    startup()
  }
  
  include(
    new EvalStackSpecs {
      def eval(str: String, debug: Boolean = false): Set[SValue] = evalE(str, debug) map { _._2 }
      
      def evalE(str: String, debug: Boolean = false) = {
        logger.debug("Beginning evaluation of query: " + str)
        val tree = compile(str)
        tree.errors must beEmpty
        val Right(dag) = decorate(emit(tree))
        withContext { ctx => 
          consumeEval("dummyUID", dag, ctx) match {
            case Success(result) => 
              logger.debug("Evaluation complete for query: " + str)
              result
            case Failure(error) => throw error
          }
        }
      }
    }
  )
  
  step {
    shutdown()
  }
  
  def startup() = ()
  
  def shutdown() = ()
}

object RawJsonStackSpecs extends ParseEvalStackSpecs[test.YId] with RawJsonColumnarTableStorageModule[test.YId] with test.YIdInstances {
  type YggConfig = ParseEvalStackSpecConfig
  object yggConfig extends ParseEvalStackSpecConfig
}
// vim: set ts=4 sw=4 et:
