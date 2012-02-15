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
package ingest
package yggdrasil 

import blueeyes.json.JsonAST._

import common.Config
import daze._

import quirrel.Compiler
import quirrel.LineErrors
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import com.precog.yggdrasil._
import com.precog.yggdrasil.shard._

import akka.actor.ActorSystem
import akka.dispatch._
import akka.util.duration._
import akka.util.Duration
import akka.util.Timeout

import scalaz.{Success, Failure, Validation}
import scalaz.effect.IO

import org.streum.configrity.Configuration

import net.lag.configgy.ConfigMap

trait YggdrasilQueryExecutorConfig extends YggEnumOpsConfig with LevelDBQueryConfig with LevelDBMemoizationConfig with BaseConfig {
  val flatMapTimeout: Duration = config[Int]("precog.evaluator.timeout.fm", 30) seconds
  val projectionRetrievalTimeout: Timeout = Timeout(config[Int]("precog.evaluator.timeout.projection", 30) seconds)
}

trait YggdrasilQueryExecutorComponent {
  import blueeyes.json.xschema.Extractor

  def loadConfig: IO[YggdrasilQueryExecutorConfig] = IO { 
    new BaseConfig with YggdrasilQueryExecutorConfig {
      val config = Configuration.parse("")  
      val sortWorkDir = scratchDir
      val memoizationBufferSize = sortBufferSize
      val memoizationWorkDir = scratchDir
    }
  }
    
  def queryExecutorFactory(queryExecutorConfig: ConfigMap): QueryExecutor = queryExecutorFactory()
  
  def queryExecutorFactory(): QueryExecutor = {

    val validatedQueryExecutor: IO[Validation[Extractor.Error, QueryExecutor]] = 
      for( yConfig <- loadConfig;
           state   <- YggState.restore(yConfig.dataDir) ) yield {

        state map { yState => new YggdrasilQueryExecutor {
          val controlTimeout = Duration(120, "seconds")
          val maxEvalDuration = controlTimeout 

          lazy val actorSystem = ActorSystem("akka_ingest_server")
          implicit lazy val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

          val yggConfig = yConfig
          val yggState = yState
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
    with DatasetConsumers
    with OperationsAPI 
    with YggdrasilEnumOpsComponent
    with LevelDBQueryComponent 
    with LevelDBMemoizationComponent { self =>

  type YggConfig = YggEnumOpsConfig with LevelDBQueryConfig with LevelDBMemoizationConfig
  
  val yggState: YggState
  val actorSystem: ActorSystem

  object storage extends ActorYggShard {
    val yggState = self.yggState
    val yggConfig: YggConfig = self.yggConfig
  }

  object ops extends Ops 

  object query extends QueryAPI 

  def startup() = storage.start
  def shutdown() = storage.stop map { _ => actorSystem.shutdown } 

  def execute(query: String) = {
    try {
      asBytecode(query) match {
        case Right(bytecode) => 
          decorate(bytecode) match {
            case Right(dag)  => JString(evaluateDag(dag))
            case Left(error) => JString("Error processing dag: %s".format(error.toString))
          }
        
        case Left(errors) => JString("Parsing errors: %s".format(errors.toString))
      }
    } catch {
      // Need to be more specific here or maybe change execute to explicitly return errors 
      case ex: Exception => JString("Error processing query: %s".format(ex.getMessage))
    }
  }

  private def evaluateDag(dag: DepGraph) = {
    consumeEval(dag) map { _._2 } map SValue.asJSON mkString ("[", ",", "]")
  }

  private def asBytecode(query: String): Either[Set[Error], Vector[Instruction]] = {
    val tree = compile(query)
    if(tree.errors.isEmpty) Right(emit(tree)) else Left(tree.errors)
  }
}

