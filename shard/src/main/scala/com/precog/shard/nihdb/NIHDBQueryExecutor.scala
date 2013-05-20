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
package com.precog.shard
package nihdb

import blueeyes.json._

import com.precog.common._

import com.precog.common.security._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.jobs._

import com.precog.daze._
import com.precog.muspelheim._
import com.precog.niflheim._
import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.nihdb._
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
}

trait NIHDBQueryExecutorComponent  {
  import blueeyes.json.serialization.Extractor

  def platformFactory(config0: Configuration, extApiKeyFinder: APIKeyFinder[Future], extAccountFinder: AccountFinder[Future], extJobManager: JobManager[Future]) = {
    new ManagedPlatform
        with ShardQueryExecutorPlatform[Future]
        with NIHDBColumnarTableModule
        with NIHDBStorageMetadataSource
        with KafkaIngestActorProjectionSystem { platform =>

      type YggConfig = NIHDBQueryExecutorConfig
      val yggConfig = new NIHDBQueryExecutorConfig {
        override val config = config0
        val sortWorkDir = scratchDir
        val memoizationBufferSize = sortBufferSize
        val memoizationWorkDir = scratchDir

        val clock = blueeyes.util.Clock.System
        val smallSliceSize = config[Int]("jdbm.small_slice_size", 8)
        val timestampRequiredAfter = new Instant(config[Long]("ingest.timestamp_required_after", 1363327426906L))

        //TODO: Get a producer ID
        val idSource = new FreshAtomicIdSource
      }

      val clock = blueeyes.util.Clock.System

      val defaultTimeout = yggConfig.maxEvalDuration

      protected lazy val queryLogger = LoggerFactory.getLogger("com.precog.shard.ShardQueryExecutor")

      private val threadPooling = new PerAccountThreadPooling(extAccountFinder)

      implicit val actorSystem = ActorSystem("nihdbExecutorActorSystem")
      implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
      implicit val M: Monad[Future] = new FutureMonad(executionContext)

      val jobActorSystem = ActorSystem("jobPollingActorSystem")

      val chefs = (1 to yggConfig.howManyChefsInTheKitchen).map { _ =>
        actorSystem.actorOf(Props(Chef(VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)), VersionedSegmentFormat(Map(1 -> V1SegmentFormat)))))
      }
      val masterChef = actorSystem.actorOf(Props[Chef].withRouter(RoundRobinRouter(chefs)))

      val accessControl = extApiKeyFinder
      val storageTimeout = yggConfig.storageTimeout
      val jobManager = extJobManager
      val metadataClient = new StorageMetadataClient[Future](this)

      val permissionsFinder = new PermissionsFinder(extApiKeyFinder, extAccountFinder, yggConfig.timestampRequiredAfter)
      val resourceBuilder = new DefaultResourceBuilder(actorSystem, clock, masterChef, yggConfig.cookThreshold, storageTimeout, permissionsFinder)
      val projectionsActor = actorSystem.actorOf(Props(new PathRoutingActor(yggConfig.dataDir, resourceBuilder, permissionsFinder, storageTimeout.duration, jobManager, clock)))
      val ingestSystem = initShardActors(permissionsFinder, projectionsActor)

      trait TableCompanion extends NIHDBColumnarTableCompanion //{
//        import scalaz.std.anyVal._
//        implicit val geq: scalaz.Equal[Int] = scalaz.Equal[Int]
//      }

      object Table extends TableCompanion

      def ingestFailureLog(checkpoint: YggCheckpoint, logRoot: File): IngestFailureLog = FilesystemIngestFailureLog(logRoot, checkpoint)

      def asyncExecutorFor(apiKey: APIKey): Future[Validation[String, QueryExecutor[Future, JobId]]] = {
        (for {
          executionContext0 <- threadPooling.getAccountExecutionContext(apiKey)
        } yield {
          new AsyncQueryExecutor {
            val executionContext: ExecutionContext = executionContext0
          }
        }).validation
      }

      def syncExecutorFor(apiKey: APIKey): Future[Validation[String, QueryExecutor[Future, (Option[JobId], StreamT[Future, Slice])]]] = {
        (for {
          executionContext0 <- threadPooling.getAccountExecutionContext(apiKey)
        } yield {
          new SyncQueryExecutor {
            val executionContext: ExecutionContext = executionContext0
          }
        }).validation
      }

      override def executor(implicit shardQueryMonad: JobQueryTFMonad): QueryExecutor[JobQueryTF, StreamT[JobQueryTF, Slice]] = {
        implicit val mn = new (Future ~> JobQueryTF) {
          def apply[A](fut: Future[A]) = fut.liftM[JobQueryT]
        }

        new ShardQueryExecutor[JobQueryTF](shardQueryMonad) {
          val M = shardQueryMonad.M
          type YggConfig = NIHDBQueryExecutorConfig
          val yggConfig = platform.yggConfig
          val queryReport = errorReport[Option[FaultPosition]](shardQueryMonad, implicitly)
          def freshIdScanner = platform.freshIdScanner
        } map { case (faults, result) =>
          result
        }
      }

      def shutdown() = for {
        _ <- Stoppable.stop(ingestSystem.map(_.stoppable).getOrElse(Stoppable.fromFuture(Future(()))))
        _ <- IngestSystem.actorStop(yggConfig, projectionsActor, "projections")
        _ <- IngestSystem.actorStop(yggConfig, masterChef, "masterChef")
        _ <- chefs.map(IngestSystem.actorStop(yggConfig, _, "masterChef")).sequence
      } yield {
        queryLogger.info("Actor ecossytem shutdown complete.")
        jobActorSystem.shutdown()
        actorSystem.shutdown()
      }
    }
  }
}
