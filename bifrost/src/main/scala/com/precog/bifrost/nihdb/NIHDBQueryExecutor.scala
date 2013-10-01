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
package com.precog.bifrost
package nihdb

import blueeyes.json._

import com.precog.common._

import com.precog.common.security._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.jobs._

import com.precog.mimir._
import com.precog.niflheim._
import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.execution._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.nihdb._
import com.precog.yggdrasil.scheduling._
import com.precog.yggdrasil.serialization._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.util._
import com.precog.yggdrasil.vfs._

import com.precog.util.FilesystemFileOps
import com.precog.util.PrecogUnit

import blueeyes.bkka._
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeExtractor => _, DateTimeDecomposer => _, _ }

import akka.actor.{ActorSystem, Props}
import akka.dispatch._
import akka.pattern.ask
import akka.pattern.GracefulStopSupport
import akka.routing.RoundRobinRouter
import akka.util.{Duration, Timeout}
import akka.util.duration._

import org.slf4j.{LoggerFactory, MDC}
import org.joda.time.Instant

import java.io.File

import scalaz._
import scalaz.Validation._
import scalaz.effect.IO
import scalaz.syntax.monad._
import scalaz.syntax.foldable._
import scalaz.syntax.bifunctor._
import scalaz.syntax.std.either._
import scalaz.syntax.traverse._
import scalaz.std.iterable._
import scalaz.std.indexedSeq._
import scalaz.std.anyVal._
import scalaz.std.list._

import org.streum.configrity.Configuration

// type NIHDBQueryExecutor

trait NIHDBQueryExecutorConfig
    extends ShardQueryExecutorConfig
    with BlockStoreColumnarTableModuleConfig
    with ManagedQueryModuleConfig
    with IdSourceConfig
    with EvaluatorConfig
    with KafkaIngestActorProjectionSystemConfig {

  lazy val flatMapTimeout: Duration = config[Int]("precog.evaluator.timeout.fm", 30) seconds
  lazy val maxEvalDuration: Duration = config[Int]("precog.evaluator.timeout.eval", 90) seconds
  lazy val jobPollFrequency: Duration = config[Int]("precog.evaluator.poll.cancellation", 3) seconds

  lazy val howManyChefsInTheKitchen: Int = config[Int]("precog.storage.chef_count", 4)
  lazy val cookThreshold: Int = config[Int]("precog.storage.cook_threshold", 20000)
  def maxSliceSize = cookThreshold
  lazy val storageTimeout: Timeout = Timeout(config[Int]("precog.storage.timeout", 300) seconds)
  lazy val quiescenceTimeout: Duration = config[Int]("precog.storage.quiescence_timeout", 300) seconds
  lazy val maxOpenPaths: Int = config[Int]("precog.storage.max_open_paths", 500)
}

trait NIHDBQueryExecutorComponent  {
  import blueeyes.json.serialization.Extractor

  def nihdbPlatform(config0: Configuration, extApiKeyFinder: APIKeyFinder[Future], extAccountFinder: AccountFinder[Future], extJobManager: JobManager[Future]) = {
    new ManagedPlatform
        with SecureVFSModule[Future, Slice]
        with ActorVFSModule
        with SchedulingActorModule
        with ShardQueryExecutorPlatform[Future]
        with VFSColumnarTableModule
        with KafkaIngestActorProjectionSystem 
        with GracefulStopSupport { platform =>

      type YggConfig = NIHDBQueryExecutorConfig
      val yggConfig = new NIHDBQueryExecutorConfig {
        override val config = config0.detach("queryExecutor")
        val sortWorkDir = scratchDir
        val memoizationBufferSize = sortBufferSize
        val memoizationWorkDir = scratchDir

        val clock = blueeyes.util.Clock.System
        val smallSliceSize = config[Int]("jdbm.small_slice_size", 8)
        val timestampRequiredAfter = new Instant(config[Long]("ingest.timestamp_required_after", 1363327426906L))
        val schedulingTimeout = new Timeout(config[Int]("scheduling.timeout_ms", 10000))

        //TODO: Get a producer ID
        val idSource = new FreshAtomicIdSource
      }

      val clock = blueeyes.util.Clock.System

      val defaultTimeout = yggConfig.maxEvalDuration

      protected lazy val queryLogger = LoggerFactory.getLogger("com.precog.bifrost.ShardQueryExecutor")

      private val threadPooling = new PerAccountThreadPooling(extAccountFinder)

      implicit val actorSystem = ActorSystem("nihdbExecutorActorSystem")
      implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
      implicit val M: Monad[Future] = new FutureMonad(executionContext)

      val jobActorSystem = ActorSystem("jobPollingActorSystem")

      val chefs = (1 to yggConfig.howManyChefsInTheKitchen).map { _ =>
        actorSystem.actorOf(Props(Chef(VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)), VersionedSegmentFormat(Map(1 -> V1SegmentFormat)))))
      }
      val masterChef = actorSystem.actorOf(Props[Chef].withRouter(RoundRobinRouter(chefs)))

      //val accessControl = extApiKeyFinder
      val storageTimeout = yggConfig.storageTimeout

      val jobManager = extJobManager
      val permissionsFinder = new PermissionsFinder(extApiKeyFinder, extAccountFinder, yggConfig.timestampRequiredAfter)
      val resourceBuilder = new ResourceBuilder(actorSystem, clock, masterChef, yggConfig.cookThreshold, storageTimeout)

      private val projectionsActor = actorSystem.actorOf(Props(new PathRoutingActor(yggConfig.dataDir, storageTimeout.duration, yggConfig.quiescenceTimeout, yggConfig.maxOpenPaths, clock)))
      val ingestSystem = initShardActors(permissionsFinder, projectionsActor)

      private val actorVFS = new ActorVFS(projectionsActor, yggConfig.storageTimeout, yggConfig.storageTimeout) 
      val vfs = new SecureVFS(actorVFS, permissionsFinder, jobManager, clock)

      private val (scheduleStorage, scheduleStorageStoppable) = MongoScheduleStorage(config0.detach("scheduling"))

      private val scheduleActor = actorSystem.actorOf(Props(new SchedulingActor(jobManager, permissionsFinder, scheduleStorage, platform, clock)))

      val scheduler = new ActorScheduler(scheduleActor, yggConfig.schedulingTimeout)

      trait TableCompanion extends VFSColumnarTableCompanion 
      object Table extends TableCompanion

      def ingestFailureLog(checkpoint: YggCheckpoint, logRoot: File): IngestFailureLog = FilesystemIngestFailureLog(logRoot, checkpoint)

      def asyncExecutorFor(apiKey: APIKey) = {
        for {
          executionContext0 <- threadPooling.getAccountExecutionContext(apiKey)
        } yield {
          new AsyncQueryExecutor {
            val executionContext: ExecutionContext = executionContext0
          }
        }
      }

      def syncExecutorFor(apiKey: APIKey) = {
        for {
          executionContext0 <- threadPooling.getAccountExecutionContext(apiKey)
        } yield {
          new SyncQueryExecutor {
            val executionContext: ExecutionContext = executionContext0
          }
        }
      }

      override def executor(implicit shardQueryMonad: JobQueryTFMonad): QueryExecutor[JobQueryTF, StreamT[JobQueryTF, Slice]] = {
        implicit val mn = new (Future ~> JobQueryTF) {
          def apply[A](fut: Future[A]) = fut.liftM[JobQueryT]
        }

        new ShardQueryExecutor[JobQueryTF](shardQueryMonad) with IdSourceScannerModule {
          val M = shardQueryMonad.M
          type YggConfig = NIHDBQueryExecutorConfig
          val yggConfig = platform.yggConfig
          val queryReport = errorReport[Option[FaultPosition]](shardQueryMonad, implicitly)
          override def freshIdScanner = platform.freshIdScanner
        } map { case (faults, result) =>
          result
        }
      }

      def shutdown() = for {
        _ <- Stoppable.stop(Stoppable.fromFuture(gracefulStop(scheduleActor, yggConfig.schedulingTimeout.duration)(actorSystem)))
        _ <- Stoppable.stop(ingestSystem.map(_.stoppable).getOrElse(Stoppable.fromFuture(Future(()))))
        _ <- IngestSystem.actorStop(yggConfig, projectionsActor, "projections")
        _ <- IngestSystem.actorStop(yggConfig, masterChef, "masterChef")
        _ <- Stoppable.stop(scheduleStorageStoppable)
        _ <- chefs.map(IngestSystem.actorStop(yggConfig, _, "masterChef")).sequence
      } yield {
        queryLogger.info("Actor ecossytem shutdown complete.")
        jobActorSystem.shutdown()
        actorSystem.shutdown()
      }
    }
  }
}
