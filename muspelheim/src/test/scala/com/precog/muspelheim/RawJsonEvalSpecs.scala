/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
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

import com.weiglewilczek.slf4s.Logging

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext

trait ParseEvalStackSpecs extends Specification 
    with ParseEvalStack
    with IterableDatasetOpsComponent
    with LevelDBQueryComponent
    with DiskIterableMemoizationComponent 
    with MemoryDatasetConsumer 
    with Logging {

  override type Dataset[A] = IterableDataset[A]
  override type Memoable[α] = Iterable[α]

  def controlTimeout = Duration(30, "seconds")      // it's just unreasonable to run tests longer than this
  
  val actorSystem = ActorSystem("platform_specs_actor_system")

  implicit def asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

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
    logger.trace("Init yggConfig")
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

  object ops extends Ops 
  object query extends QueryAPI 

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

object RawJsonStackSpecs extends ParseEvalStackSpecs with RawJsonShardComponent { 
  object storage extends Storage
}
// vim: set ts=4 sw=4 et:
