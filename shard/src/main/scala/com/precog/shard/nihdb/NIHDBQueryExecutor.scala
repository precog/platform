package com.precog.shard
package nihdb

import blueeyes.json._

import com.precog.common._
import com.precog.common.json._
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

import java.io.File
import java.nio.CharBuffer

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

      val projectionsActor = actorSystem.actorOf(Props(new NIHDBProjectionsActor(yggConfig.dataDir, yggConfig.archiveDir, FilesystemFileOps, masterChef, yggConfig.cookThreshold, storageTimeout, accessControl)))

      val shardActors @ ShardActors(ingestSupervisor, _) =
        initShardActors(extAccountFinder, projectionsActor)

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

      def syncExecutorFor(apiKey: APIKey): Future[Validation[String, QueryExecutor[Future, (Option[JobId], StreamT[Future, CharBuffer])]]] = {
        (for {
          executionContext0 <- threadPooling.getAccountExecutionContext(apiKey)
        } yield {
          new SyncQueryExecutor {
            val executionContext: ExecutionContext = executionContext0
          }
        }).validation
      }

      override def executor(implicit shardQueryMonad: ShardQueryMonad): QueryExecutor[ShardQuery, StreamT[ShardQuery, CharBuffer]] = {
        implicit val mn = new (Future ~> ShardQuery) {
          def apply[A](fut: Future[A]) = fut.liftM[JobQueryT]
        }

        new ShardQueryExecutor[ShardQuery](shardQueryMonad) with IdSourceScannerModule {
          val M = shardQueryMonad.M
          type YggConfig = NIHDBQueryExecutorConfig
          val yggConfig = platform.yggConfig
          val queryReport = errorReport[Option[FaultPosition]](shardQueryMonad, implicitly)
        }
      }

      def shutdown() = for {
        _ <- Stoppable.stop(shardActors.stoppable)
        _ <- ShardActors.actorStop(yggConfig, projectionsActor, "projections")
        _ <- ShardActors.actorStop(yggConfig, masterChef, "masterChef")
        _ <- chefs.map(ShardActors.actorStop(yggConfig, _, "masterChef")).sequence
      } yield {
        queryLogger.info("Actor ecossytem shutdown complete.")
        jobActorSystem.shutdown()
        actorSystem.shutdown()
      }
    }
  }
}
