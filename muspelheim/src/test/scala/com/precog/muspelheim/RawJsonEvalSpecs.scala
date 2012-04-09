package com.precog
package muspelheim

import common.VectorCase
import common.kafka._

import daze._
import daze.memoization._
import daze.util._

import quirrel._
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import yggdrasil._
import yggdrasil.actor._
import yggdrasil.serialization._
import muspelheim._

import org.specs2.mutable._
  
import akka.dispatch.Await
import akka.util.Duration

import java.io.File

import scalaz._
import scalaz.effect.IO

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext

trait ParseEvalStackSpecs extends Specification 
    with ParseEvalStack
    with IterableDatasetOpsComponent
    with LevelDBQueryComponent
    with DiskIterableMemoizationComponent 
    with MemoryDatasetConsumer {

  override type Dataset[A] = IterableDataset[A]
  override type Memoable[α] = Iterable[α]

  lazy val controlTimeout = Duration(30, "seconds")      // it's just unreasonable to run tests longer than this
  
  lazy val actorSystem = ActorSystem("platform_specs_actor_system")
  implicit lazy val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

  def dataset(idCount: Int, data: Iterable[(Identities, Seq[CValue])]) = IterableDataset(idCount, data)

  trait YggConfig extends 
    BaseConfig with 
    YggEnumOpsConfig with 
    LevelDBQueryConfig with 
    DiskMemoizationConfig with 
    DatasetConsumersConfig with
    IterableDatasetOpsConfig with 
    ProductionActorConfig

  object yggConfig extends YggConfig {
    lazy val config = Configuration parse {
      Option(System.getProperty("precog.storage.root")) map { "precog.storage.root = " + _ } getOrElse { "" }
    }

    lazy val sortWorkDir = scratchDir
    lazy val memoizationBufferSize = sortBufferSize
    lazy val memoizationWorkDir = scratchDir

    lazy val flatMapTimeout = Duration(100, "seconds")
    lazy val projectionRetrievalTimeout = akka.util.Timeout(Duration(10, "seconds"))
    lazy val maxEvalDuration = controlTimeout
    lazy val clock = blueeyes.util.Clock.System

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

  lazy val Success(shardState) = YggState.restore(yggConfig.dataDir).unsafePerformIO

  object ops extends Ops 
  object query extends QueryAPI 

  step {
    startup()
  }
  
  include(
    new EvalStackSpecs {
      def eval(str: String, debug: Boolean = false): Set[SValue] = evalE(str, debug) map { _._2 }
      
      def evalE(str: String, debug: Boolean = false) = {
        val tree = compile(str)
        tree.errors must beEmpty
        val Right(dag) = decorate(emit(tree))
        withContext { ctx => 
          consumeEval("dummyUID", dag, ctx) match {
            case Success(result) => result
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

object RawJsonStackSpecs extends ParseEvalStackSpecs with RawJsonShardComponent { 
  object storage extends Storage
}
// vim: set ts=4 sw=4 et:
