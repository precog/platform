package com.precog
package shard
package yggdrasil 

import blueeyes.json.JsonAST._

import daze._

import quirrel.Compiler
import quirrel.LineErrors
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._

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
    YggEnumOpsConfig with 
    LevelDBQueryConfig with 
    DiskMemoizationConfig with 
    ProductionActorConfig with
    DatasetConsumersConfig with
    BaseConfig {
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
      val chunkSerialization = BinaryProjectionSerialization
      val memoizationBufferSize = sortBufferSize
      val memoizationWorkDir = scratchDir
    }
  }
    
  def queryExecutorFactory(config: Configuration): QueryExecutor = {
    val yConfig = wrapConfig(config)
    val validatedQueryExecutor: IO[Validation[Extractor.Error, QueryExecutor]] = 
      for( state <- YggState.restore(yConfig.dataDir) ) yield {

        state map { yState => new YggdrasilQueryExecutor {
          trait Storage extends ActorYggShard with ProductionActorEcosystem
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
    with LineErrors
    with Compiler
    with Parser
    with TreeShaker
    with ProvenanceChecker
    with Emitter
    with Evaluator
    with MemoryDatasetConsumer
    with OperationsAPI
    with YggdrasilEnumOpsComponent
    with LevelDBQueryComponent 
    with DiskMemoizationComponent 
    with Logging { self =>

  type YggConfig = YggdrasilQueryExecutorConfig
  type Storage <: ActorYggShard

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

