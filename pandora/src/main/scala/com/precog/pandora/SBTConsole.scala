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
package pandora

import common.kafka._
import common.security._

import daze._

import pandora._

import com.precog.common.Path
import com.precog.common.accounts._

import quirrel._
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import yggdrasil._
import yggdrasil.actor._
import yggdrasil.jdbm3._
import yggdrasil.metadata._
import yggdrasil.serialization._
import yggdrasil.table._
import yggdrasil.util._

import com.precog.util.FilesystemFileOps

import akka.actor.ActorSystem
import akka.actor.Props
import akka.dispatch._
import akka.util.Duration

import com.codecommit.gll.LineStream

import akka.dispatch.Await
import akka.util.Duration
import scalaz._

import java.io.File

import scalaz.effect.IO

import blueeyes.bkka._

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

trait PlatformConfig extends BaseConfig
    with IdSourceConfig
    with EvaluatorConfig
    with StandaloneShardSystemConfig
    with ColumnarTableModuleConfig
    with BlockStoreColumnarTableModuleConfig
    with JDBMProjectionModuleConfig
    with ActorStorageModuleConfig
    with ActorProjectionModuleConfig 

trait Platform extends muspelheim.ParseEvalStack[Future] 
    with IdSourceScannerModule
    with SliceColumnarTableModule[Future, Array[Byte]]
    with ActorStorageModule
    with ActorProjectionModule[Array[Byte], Slice]
    with StandaloneActorProjectionSystem 
    with LongIdMemoryDatasetConsumer[Future] 
    with PrettyPrinter { self =>

  type YggConfig = PlatformConfig

  trait TableCompanion extends SliceColumnarTableCompanion {
    import scalaz.std.anyVal._
    implicit val geq: scalaz.Equal[Int] = scalaz.Equal[Int]
  }

  object Table extends TableCompanion
}

object SBTConsole {
  val controlTimeout = Duration(30, "seconds")

  val platform = new Platform { console =>
    implicit val actorSystem = ActorSystem("sbtConsoleActorSystem")
    implicit val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
    implicit val M: Monad[Future] with Copointed[Future] = new blueeyes.bkka.FutureMonad(asyncContext) with Copointed[Future] {
      def copoint[A](m: Future[A]) = Await.result(m, yggConfig.maxEvalDuration)
    }

    val yggConfig = new PlatformConfig {
      val config = Configuration parse {
        Option(System.getProperty("precog.storage.root")) map { "precog.storage.root = " + _ } getOrElse { "" }
      }

      val sortWorkDir = scratchDir
      val memoizationBufferSize = sortBufferSize
      val memoizationWorkDir = scratchDir

      val flatMapTimeout = controlTimeout
      val maxEvalDuration = controlTimeout
      val clock = blueeyes.util.Clock.System
      val ingestConfig = None
      
      val maxSliceSize = 10000
      val smallSliceSize = 8

      //TODO: Get a producer ID
      val idSource = new FreshAtomicIdSource
    }

    val metadataStorage = FileMetadataStorage.load(yggConfig.dataDir, yggConfig.archiveDir, FilesystemFileOps).unsafePerformIO

    val projectionsActor = actorSystem.actorOf(Props(new ProjectionsActor), "projections")
    val shardActors @ ShardActors(ingestSupervisor, metadataActor, metadataSync) =
      initShardActors(metadataStorage, AccountFinder.Empty[Future], projectionsActor)

    val accessControl = new UnrestrictedAccessControl[Future]()

    object Projection extends ProjectionCompanion(projectionsActor, yggConfig.metadataTimeout)
    class Storage extends ActorStorageLike(actorSystem, ingestSupervisor, metadataActor)
    val storage = new Storage

    def userMetadataView(apiKey: APIKey) = storage.userMetadataView(apiKey)

    val rawProjectionModule = new JDBMProjectionModule {
      type YggConfig = PlatformConfig
      val yggConfig = console.yggConfig
      val Projection = new ProjectionCompanion {
        def fileOps = FilesystemFileOps
        def ensureBaseDir(descriptor: ProjectionDescriptor): IO[File] = metadataStorage.ensureDescriptorRoot(descriptor)
        def findBaseDir(descriptor: ProjectionDescriptor): Option[File] = metadataStorage.findDescriptorRoot(descriptor)
        def archiveDir(descriptor: ProjectionDescriptor): IO[Option[File]] = metadataStorage.findArchiveRoot(descriptor)
      }
    }

    def Evaluator[N[+_]](N0: Monad[N])(implicit mn: Future ~> N, nm: N ~> Future): EvaluatorLike[N] = 
      new Evaluator[N](N0) with IdSourceScannerModule {
        type YggConfig = PlatformConfig
        val yggConfig = console.yggConfig
        val report = LoggingQueryLogger[N, instructions.Line](N0)
      }

    def eval(str: String): Set[SValue] = evalE(str)  match {
      case Success(results) => results.map(_._2)
      case Failure(t) => throw t
    }

    def evalE(str: String) = {
      val dag = produceDAG(str)
      consumeEval("dummyAPIKey", dag, Path.Root) 
    }
    
    def produceDAG(str: String) = {
      val forest = compile(str)
      val validForest = forest filter { _.errors.isEmpty }
      
      if (validForest.isEmpty) {
        val strs = forest map { tree =>
          tree.errors map showError mkString ("Set(\"", "\", \"", "\")")
        }
        
        sys.error(strs mkString " | ")
      }
      
      if (validForest.size > 1) {
        sys.error("ambiguous parse (good luck!)")
      }
      
      val tree = validForest.head
      val Right(dag) = decorate(emit(tree))
      dag
    }

    def printDAG(str: String) = {
      val dag = produceDAG(str)
      prettyPrint(dag)
    }

    def startup() {
      // start storage shard 
    }
    
    def shutdown() {
      // stop storage shard
      Await.result(Stoppable.stop(shardActors.stoppable, yggConfig.stopTimeout.duration), yggConfig.stopTimeout.duration)
      actorSystem.shutdown()
    }
  }
}

// vim: set ts=4 sw=4 et:
