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
    with StorageModule[M]
    with MemoryDatasetConsumer[M] 
    with IdSourceScannerModule[M] {

  protected lazy val parseEvalLogger = LoggerFactory.getLogger("com.precog.muspelheim.ParseEvalStackSpecs")

  val sliceSize = 10
  
  def controlTimeout = Duration(120, "seconds")      // it's just unreasonable to run tests longer than this
  
  implicit val actorSystem = ActorSystem("platformSpecsActorSystem")

  implicit def asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

  type YggConfig <: EvaluatorConfig with IdSourceConfig
  
  class ParseEvalStackSpecConfig extends BaseConfig with IdSourceConfig {
    parseEvalLogger.trace("Init yggConfig")
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
    
    val maxSliceSize = 10

    val idSource = new IdSource {
      private val source = new java.util.concurrent.atomic.AtomicLong
      def nextId() = source.getAndIncrement
    }
  }

  include(
    new EvalStackSpecs {
      def eval(str: String, debug: Boolean = false): Set[SValue] = evalE(str, debug) map { _._2 }
      
      def evalE(str: String, debug: Boolean = false): Set[SEvent] = {
        parseEvalLogger.debug("Beginning evaluation of query: " + str)
        
        val forest = compile(str) filter { _.errors.isEmpty }
        forest must haveSize(1)
        
        val tree = forest.head
        
        val Right(dag) = decorate(emit(tree))
        withContext { ctx => 
          consumeEval("dummyUID", dag, ctx, Path.Root) match {
            case Success(result) => 
              parseEvalLogger.debug("Evaluation complete for query: " + str)
              result
            case Failure(error) => throw error
          }
        }
      }
    }
  )
  
  include(
    "full stack rendering" should {
      def evalTable(str: String, debug: Boolean = false): Table = {
        import trans._
        
        parseEvalLogger.debug("Beginning evaluation of query: " + str)
        
        val forest = compile(str) filter { _.errors.isEmpty }
        forest must haveSize(1)
        
        val tree = forest.head
        tree.errors must beEmpty
        val Right(dag) = decorate(emit(tree))
        withContext { ctx => 
          val tableM = eval("dummyUID", dag, ctx, Path.Root, true)
          tableM map { _ transform DerefObjectStatic(Leaf(Source), CPathField("value")) } copoint
        }
      }
      
      "render a set of numbers interleaved by delimiters" in {
        val stream = evalTable("//tutorial/transactions.quantity") renderJson ','
        val strings = stream map { _.toString }
        val str = strings.foldLeft("") { _ + _ } copoint
        
        str must contain(",")
      }
    }
  )
}

/*
object RawJsonStackSpecs extends ParseEvalStackSpecs[Free.Trampoline] with RawJsonColumnarTableStorageModule[Free.Trampoline] {
  implicit val M = Trampoline.trampolineMonad
  type YggConfig = ParseEvalStackSpecConfig
  object yggConfig extends ParseEvalStackSpecConfig

  object Table extends TableCompanion {
    implicit val geq: scalaz.Equal[Int] = intInstance
  }
}
*/
// vim: set ts=4 sw=4 et:
