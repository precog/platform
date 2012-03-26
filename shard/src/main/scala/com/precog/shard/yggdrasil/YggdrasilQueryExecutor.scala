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
package shard
package yggdrasil 

import blueeyes.json.JsonAST._

import daze._
import daze.memoization._

import pandora.ParseEvalStack

import com.precog.yggdrasil._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.serialization._

import akka.actor.ActorSystem
import akka.dispatch._
import akka.util.duration._
import akka.util.Duration
import akka.util.Timeout

import com.weiglewilczek.slf4s.Logging

import scalaz.{Success, Failure, Validation}
import scalaz.Validation._
import scalaz.effect.IO
import scalaz.syntax.monad._

import org.streum.configrity.Configuration

trait YggdrasilQueryExecutorConfig extends 
    BaseConfig with 
    YggEnumOpsConfig with 
    LevelDBQueryConfig with 
    DiskMemoizationConfig with 
    DatasetConsumersConfig with 
    IterableDatasetOpsConfig with 
    ProductionActorConfig {
  lazy val flatMapTimeout: Duration = config[Int]("precog.evaluator.timeout.fm", 30) seconds
  lazy val projectionRetrievalTimeout: Timeout = Timeout(config[Int]("precog.evaluator.timeout.projection", 30) seconds)
  lazy val maxEvalDuration: Duration = config[Int]("precog.evaluator.timeout.eval", 90) seconds
}

trait YggdrasilQueryExecutorComponent {
  import blueeyes.json.xschema.Extractor

  private def wrapConfig(wrappedConfig: Configuration) = {
    new YggdrasilQueryExecutorConfig {
      val config = wrappedConfig 
      val sortWorkDir = scratchDir
      val memoizationBufferSize = sortBufferSize
      val memoizationWorkDir = scratchDir

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
  }
    
  def queryExecutorFactory(config: Configuration): QueryExecutor = {
    val yConfig = wrapConfig(config)
    val validatedQueryExecutor: IO[Validation[Extractor.Error, QueryExecutor]] = 
      for( state <- YggState.restore(yConfig.dataDir) ) yield {

        state map { yState => 
          new YggdrasilQueryExecutor
            with IterableDatasetOpsComponent
            with LevelDBQueryComponent
            with DiskIterableDatasetMemoizationComponent {
          override type Dataset[E] = IterableDataset[E]
          trait Storage extends ActorYggShard[IterableDataset] with ProductionActorEcosystem
          lazy val actorSystem = ActorSystem("yggdrasil_exeuctor_actor_system")
          implicit lazy val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
          val yggConfig = yConfig

          object ops extends Ops 
          object query extends QueryAPI 
          val storage = new Storage {
            type YggConfig = YggdrasilQueryExecutorConfig
            lazy val yggConfig = yConfig
            lazy val yggState = yState
          }
        }}
      }

    validatedQueryExecutor map { 
      case Success(qs) => qs
      case Failure(er) => sys.error("Error initializing query service: " + er)
    } unsafePerformIO
  }
}

trait YggdrasilQueryExecutor 
    extends QueryExecutor
    with ParseEvalStack
    with LevelDBQueryComponent 
    with MemoryDatasetConsumer
    with DiskIterableDatasetMemoizationComponent
    with Logging  { self =>
  override type Dataset[E] = IterableDataset[E]

  type YggConfig = YggdrasilQueryExecutorConfig
  type Storage <: ActorYggShard[IterableDataset]

  def startup() = storage.actorsStart
  def shutdown() = storage.actorsStop

  case class StackException(error: StackError) extends Exception(error.toString)

  def execute(userUID: String, query: String): Validation[EvaluationError, JArray] = {
    import EvaluationError._
    implicit val M = Validation.validationMonad[EvaluationError]

    val solution: Validation[Throwable, Validation[EvaluationError, JArray]] = Validation.fromTryCatch {
      asBytecode(query) flatMap { bytecode =>
        Validation.fromEither(decorate(bytecode)).bimap(
          error => systemError(StackException(error)),
          dag   => evaluateDag(userUID, dag).fail.map(systemError(_)).validation
        ).join
      }
    } 

    solution.fail.map(systemError(_)).validation.join
  }

  def metadata(userUID: String): MetadataView = {
    storage.userMetadataView(userUID)
  }

  private def evaluateDag(userUID: String, dag: DepGraph): Validation[Throwable, JArray] = {
    consumeEval(userUID, dag) map { events => JArray(events.map(_._2.toJValue)(collection.breakOut)) }
  }

  private def asBytecode(query: String): Validation[EvaluationError, Vector[Instruction]] = {
    try {
      val tree = compile(query)
      if (tree.errors.isEmpty) success(emit(tree)) 
      else failure(
        UserError(
          JArray(
            (tree.errors: Set[Error]) map {
              case Error(loc, tp) =>
                JObject(
                  JField("message", JString("Errors occurred compiling your query.")) 
                  :: JField("line", JString(loc.line))
                  :: JField("lineNum", JInt(loc.lineNum))
                  :: JField("colNum", JInt(loc.colNum))
                  :: JField("detail", JString(tp.toString))
                  :: Nil
                )
            } toList
          )
        )
      )
    } catch {
      case ex: ParseException => failure(
        UserError(
          JArray(
            JObject(
              JField("message", JString("An error occurred parsing your query."))
              :: JField("line", JString(ex.failures.head.tail.line))
              :: JField("lineNum", JInt(ex.failures.head.tail.lineNum))
              :: JField("colNum", JInt(ex.failures.head.tail.colNum))
              :: JField("detail", JString(ex.mkString))
              :: Nil
            ) :: Nil
          )
        )
      )
    }
  }
}

