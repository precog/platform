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

import edu.uwm.cs.gll.LineStream

import com.precog._
import com.precog.yggdrasil.shard._
import com.precog.common.kafka._

import daze._
import daze.util._

import pandora._

import quirrel._
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import yggdrasil._
import yggdrasil.shard._

object SBTConsole {
  val platform = new Compiler with LineErrors with ProvenanceChecker with Emitter with Evaluator with DatasetConsumers with OperationsAPI with AkkaIngestServer with YggdrasilEnumOpsComponent with LevelDBQueryComponent with DiskMemoizationComponent with DAGPrinter {
    import akka.dispatch.Await
    import akka.util.Duration
    import scalaz._

    import java.io.File

    import scalaz.effect.IO
    
    import org.streum.configrity.Configuration
    import org.streum.configrity.io.BlockFormat

    lazy val controlTimeout = Duration(30, "seconds")

    trait YggConfig extends BaseConfig with YggEnumOpsConfig with LevelDBQueryConfig with DiskMemoizationConfig with DatasetConsumersConfig
    object yggConfig extends YggConfig {
      lazy val config = Configuration parse {
        Option(System.getProperty("precog.storage.root")) map { "precog.storage.root = " + _ } getOrElse { "" }
      }

      lazy val flatMapTimeout = controlTimeout
      lazy val projectionRetrievalTimeout = akka.util.Timeout(controlTimeout)
      lazy val chunkSerialization = SimpleProjectionSerialization
      lazy val sortWorkDir = scratchDir
      lazy val memoizationBufferSize = sortBufferSize
      lazy val memoizationWorkDir = scratchDir
      lazy val maxEvalDuration = controlTimeout
    }

    val Success(shardState) = YggState.restore(yggConfig.dataDir).unsafePerformIO
    
    type Storage = ActorYggShard
    object storage extends ActorYggShard {
      val yggState = shardState
      val yggCheckpoints = new TestYggCheckpoints
      val batchConsumer = BatchConsumer.NullBatchConsumer
    }
    
    object ops extends Ops 
    
    object query extends QueryAPI 

    def eval(str: String): Set[SValue] = evalE(str) map { _._2 }

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
      Await.result(storage.start, controlTimeout)
    }
    
    def shutdown() {
      // stop storage shard
      Await.result(storage.stop, controlTimeout)
      
      actorSystem.shutdown()
    }
  }
}

// vim: set ts=4 sw=4 et:
