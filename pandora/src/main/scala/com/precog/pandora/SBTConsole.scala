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
package com.precog.pandora

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext

import com.codecommit.gll.LineStream

import com.precog._

import common.kafka._

import daze._
import daze.util._

import pandora._

import quirrel._
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import yggdrasil._
import yggdrasil.actor._

object SBTConsole {
  
  trait Platform  extends ParseEvalStack 
                  with YggdrasilEnumOpsComponent 
                  with LevelDBQueryComponent 
                  with DiskMemoizationComponent 
                  with DAGPrinter {

    trait YggConfig extends BaseConfig 
                    with YggEnumOpsConfig 
                    with LevelDBQueryConfig 
                    with DiskMemoizationConfig 
                    with DatasetConsumersConfig 
                    with ProductionActorConfig
  }

  val platform = new Platform { console =>
    import akka.dispatch.Await
    import akka.util.Duration
    import scalaz._

    import java.io.File

    import scalaz.effect.IO
    
    import org.streum.configrity.Configuration
    import org.streum.configrity.io.BlockFormat

    lazy val actorSystem = ActorSystem("sbt_console_actor_system")
    implicit lazy val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

    lazy val controlTimeout = Duration(30, "seconds")

    object yggConfig extends YggConfig {
      lazy val config = Configuration parse {
        Option(System.getProperty("precog.storage.root")) map { "precog.storage.root = " + _ } getOrElse { "" }
      }

      lazy val flatMapTimeout = controlTimeout
      lazy val projectionRetrievalTimeout = akka.util.Timeout(controlTimeout)
      lazy val chunkSerialization = BinaryProjectionSerialization
      lazy val sortWorkDir = scratchDir
      lazy val memoizationBufferSize = sortBufferSize
      lazy val memoizationWorkDir = scratchDir
      lazy val maxEvalDuration = controlTimeout
    }

    val Success(shardState) = YggState.restore(yggConfig.dataDir).unsafePerformIO
    
    type Storage = ActorYggShard
    val storage = new ActorYggShard with StandaloneActorEcosystem {
      type YggConfig = console.YggConfig
      val yggConfig = console.yggConfig
      val yggState = shardState
    }
    
    object ops extends Ops 
    
    object query extends QueryAPI 

    def eval(str: String): Set[SValue] = evalE(str)  match {
      case Success(results) => results.map(_._2)
      case Failure(t) => throw t
    }

    def evalE(str: String) = {
      val tree = compile(str)
      if (!tree.errors.isEmpty) {
        sys.error(tree.errors map showError mkString ("Set(\"", "\", \"", "\")"))
      }
      val Right(dag) = decorate(emit(tree))
      consumeEval("0", dag)
    }

    def startup() {
      // start storage shard 
      Await.result(storage.actorsStart, controlTimeout)
    }
    
    def shutdown() {
      // stop storage shard
      Await.result(storage.actorsStop, controlTimeout)
      
      actorSystem.shutdown()
    }
  }
}

// vim: set ts=4 sw=4 et:
