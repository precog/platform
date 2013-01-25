package com.precog.shard
package jdbm3 

import blueeyes.json._

import com.precog.accounts.BasicAccountManager

import com.precog.common.json._
import com.precog.common.security._
import com.precog.common.jobs._

import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.jdbm3._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.serialization._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.util._

import com.precog.daze._
import com.precog.muspelheim._

import com.precog.common._
import com.precog.util.FilesystemFileOps
import com.precog.util.PrecogUnit

import blueeyes.bkka._
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeExtractor => _, DateTimeDecomposer => _, _ }

import akka.actor.ActorSystem
import akka.actor.Props
import akka.dispatch._
import akka.pattern.ask
import akka.util.duration._
import akka.util.Duration
import akka.util.Timeout

import org.slf4j.{LoggerFactory, MDC}

import java.io.File
import java.nio.CharBuffer

import scalaz._
import scalaz.Validation._
import scalaz.effect.IO
import scalaz.syntax.monad._
import scalaz.syntax.bifunctor._
import scalaz.syntax.std.either._

import org.streum.configrity.Configuration

trait JDBMQueryExecutorConfig
    extends ShardQueryExecutorConfig
    with BlockStoreColumnarTableModuleConfig 
    with JDBMProjectionModuleConfig
    with ManagedQueryModuleConfig
    with ActorStorageModuleConfig
    with ActorProjectionModuleConfig 
    with IdSourceConfig 
    with KafkaIngestActorProjectionSystemConfig {
      
  lazy val flatMapTimeout: Duration = config[Int]("precog.evaluator.timeout.fm", 30) seconds
  lazy val maxEvalDuration: Duration = config[Int]("precog.evaluator.timeout.eval", 90) seconds
  lazy val jobPollFrequency: Duration = config[Int]("precog.evaluator.poll.cancellation", 3) seconds

  def ingestFailureLogRoot: File
}


trait JDBMQueryExecutorComponent  {
  import blueeyes.json.serialization.Extractor

  def platformFactory(
      config0: Configuration,
      extAccessControl: APIKeyManager[Future],
      extAccountManager: BasicAccountManager[Future],
      extJobManager: JobManager[Future]) = {
    new ManagedPlatform 
        with KafkaIngestActorProjectionSystem
        with PerAccountThreadPoolModule  
        with ActorStorageModule
        with ActorProjectionModule[Array[Byte], table.Slice] { platform =>

      type YggConfig = JDBMQueryExecutorConfig
      val yggConfig = new JDBMQueryExecutorConfig {
        override val config = config0
        val sortWorkDir = scratchDir
        val memoizationBufferSize = sortBufferSize
        val memoizationWorkDir = scratchDir

        val clock = blueeyes.util.Clock.System
        val maxSliceSize = config[Int]("jdbm.max_slice_size", 10000)
        val ingestFailureLogRoot = new File(config[String]("ingest.failure_log_root"))
        val smallSliceSize = config[Int]("jdbm.small_slice_size", 8)

        //TODO: Get a producer ID
        val idSource = new FreshAtomicIdSource
      }

      val clock = blueeyes.util.Clock.System

      protected lazy val queryLogger = LoggerFactory.getLogger("com.precog.shard.ShardQueryExecutor")
      
      private implicit val actorSystem = ActorSystem("jdbmExecutorActorSystem")
      implicit val defaultAsyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
      implicit val M: Monad[Future] = new FutureMonad(defaultAsyncContext)

      val jobActorSystem = ActorSystem("jobPollingActorSystem")

      val apiKeyManager = extAccessControl
      val accountManager = extAccountManager
      val jobManager = extJobManager
      val accessControl = extAccessControl

      val metadataStorage = FileMetadataStorage.load(yggConfig.dataDir, yggConfig.archiveDir, FilesystemFileOps).unsafePerformIO

      val projectionsActor = actorSystem.actorOf(Props(new ProjectionsActor), "projections")
      val shardActors @ ShardActors(ingestSupervisor, metadataActor, metadataSync) = 
        initShardActors(metadataStorage, extAccountManager, projectionsActor)

      class Storage extends ActorStorageLike(actorSystem, ingestSupervisor, metadataActor)
      val storage = new Storage

      val rawProjectionModule = new JDBMProjectionModule {
        type YggConfig = platform.YggConfig
        val yggConfig = platform.yggConfig
        val Projection = new ProjectionCompanion {
          val fileOps = FilesystemFileOps

          def ensureBaseDir(descriptor: ProjectionDescriptor) = metadataStorage.ensureDescriptorRoot(descriptor)
          def findBaseDir(descriptor: ProjectionDescriptor) = metadataStorage.findDescriptorRoot(descriptor)
          def archiveDir(descriptor: ProjectionDescriptor) = metadataStorage.findArchiveRoot(descriptor)
        }
      }

      val Projection = new ProjectionCompanion(projectionsActor, yggConfig.projectionRetrievalTimeout)

      val metadataClient = new MetadataClient[Future] {
        def browse(userUID: String, path: Path): Future[Validation[String, JArray]] = {
          storage.userMetadataView(userUID).findChildren(path) map {
            case paths => success(JArray(paths.map( p => JString(p.toString)).toSeq: _*))
          }
        }

        def structure(userUID: String, path: Path): Future[Validation[String, JObject]] = {
          val futRoot = storage.userMetadataView(userUID).findPathMetadata(path, CPath(""))

          def transform(children: Set[PathMetadata]): JObject = {
            // Rewrite with collect or fold?
            val (primitives, compounds) = children.partition {
              case PathValue(_, _, _) => true
              case _                  => false
            }

            val fields = compounds.map {
              case PathIndex(i, children) =>
                val path = "[%d]".format(i)
                JField(path, transform(children))
              case PathField(f, children) =>
                val path = "." + f
                JField(path, transform(children))
              case _ => throw new MatchError("Non-compound in compounds")
            }.toList

            val types = JArray(primitives.map { 
              case PathValue(t, _, _) => JString(CType.nameOf(t))
              case _ => throw new MatchError("Non-primitive in primitives")
            }.toList)

            JObject(fields :+ JField("types", types))
          }

          futRoot.map { pr => Success(transform(pr.children)) } 
        }
      }

      def ingestFailureLog(checkpoint: YggCheckpoint): IngestFailureLog = {
        FilesystemIngestFailureLog(yggConfig.ingestFailureLogRoot, checkpoint)
      }

      def asyncExecutorFor(apiKey: APIKey): Future[Validation[String, QueryExecutor[Future, JobId]]] = {
        (for {
          executionContext0 <- getAccountExecutionContext(apiKey)
        } yield {
          new AsyncQueryExecutor {
            val executionContext: ExecutionContext = executionContext0
          }
        }).validation
      }

      def syncExecutorFor(apiKey: APIKey): Future[Validation[String, QueryExecutor[Future, (Option[JobId], StreamT[Future, CharBuffer])]]] = {
        (for {
          executionContext0 <- getAccountExecutionContext(apiKey)
        } yield {
          new SyncQueryExecutor {
            val executionContext: ExecutionContext = executionContext0
          }
        }).validation
      }

      override def executor(implicit shardQueryMonad: ShardQueryMonad): QueryExecutor[ShardQuery, StreamT[ShardQuery, CharBuffer]] = {
        new ShardQueryExecutor[ShardQuery] with SliceColumnarTableModule[ShardQuery, Array[Byte]] {
          type YggConfig = JDBMQueryExecutorConfig

          implicit val M0 = platform.M
          val M = shardQueryMonad

          def userMetadataView(apiKey: APIKey) = storage.userMetadataView(apiKey).liftM[JobQueryT]

          class Projection(delegate: platform.Projection) extends ProjectionLike[ShardQuery, Array[Byte], Slice] {
            def descriptor = delegate.descriptor
            def getBlockAfter(id: Option[Array[Byte]], columns: Set[ColumnDescriptor])(implicit M: Monad[ShardQuery]): ShardQuery[Option[BlockProjectionData[Array[Byte], Slice]]] = {
              delegate.getBlockAfter(id, columns).liftM[JobQueryT]
            }
          }

          class ProjectionCompanion extends ProjectionCompanionLike[ShardQuery] {
            def apply(descriptor: ProjectionDescriptor) = {
              platform.M.map(platform.Projection(descriptor))(new Projection(_)).liftM[JobQueryT]
            }
          }

          val Projection = new ProjectionCompanion

          trait TableCompanion extends SliceColumnarTableCompanion {
            import scalaz.std.anyVal._
            implicit val geq: scalaz.Equal[Int] = scalaz.Equal[Int]
          }

          object Table extends TableCompanion

          val yggConfig = platform.yggConfig
          val report = errorReport[instructions.Line](M, implicitly)
        }
      }

      def shutdown() = for {
        _ <- Stoppable.stop(shardActors.stoppable)
        _ <- ShardActors.actorStop(yggConfig, projectionsActor, "projections")
      } yield {
        queryLogger.info("Actor ecossytem shutdown complete.")
        jobActorSystem.shutdown()
        actorSystem.shutdown()
      }
    }
  }
}
