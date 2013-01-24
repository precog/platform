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
import com.precog.yggdrasil.table.jdbm3._
import com.precog.yggdrasil.util._

import com.precog.daze._
import com.precog.muspelheim.ParseEvalStack

import com.precog.common._
import com.precog.util.FilesystemFileOps
import com.precog.util.PrecogUnit

import blueeyes.json.serialization.DefaultSerialization.{ DateTimeExtractor => _, DateTimeDecomposer => _, _ }

import akka.actor.ActorSystem
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

trait BaseJDBMQueryExecutorConfig
    extends ShardQueryExecutorConfig
    with BlockStoreColumnarTableModuleConfig 
    with JDBMProjectionModuleConfig
    with ManagedQueryModuleConfig
    with IdSourceConfig {
      
  lazy val flatMapTimeout: Duration = config[Int]("precog.evaluator.timeout.fm", 30) seconds
  lazy val projectionRetrievalTimeout: Timeout = Timeout(config[Int]("precog.evaluator.timeout.projection", 30) seconds)
  lazy val maxEvalDuration: Duration = config[Int]("precog.evaluator.timeout.eval", 90) seconds
  lazy val jobPollFrequency: Duration = config[Int]("precog.evaluator.poll.cancellation", 3) seconds
}

trait JDBMQueryExecutorConfig extends BaseJDBMQueryExecutorConfig with ProductionShardSystemConfig {
  def ingestFailureLogRoot: File
}

trait JDBMQueryExecutorComponent extends ProductionActorProjectionSystem {
  import blueeyes.json.serialization.Extractor

  def queryExecutorFactoryFactory(config: Configuration,
      extAccessControl: APIKeyManager[Future],
      extAccountManager: BasicAccountManager[Future],
      extJobManager: JobManager[Future]): ManagedQueryExecutorFactory = {

    val executorConfig = new JDBMQueryExecutorConfig {
      val config = wrappedConfig 
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

    val actors = new ProductionShardSystemActorModule {
      def ingestFailureLog(checkpoint: YggCheckpoint): IngestFailureLog = {
        FilesystemIngestFailureLog(yggConfig.ingestFailureLogRoot, checkpoint)
      }
    }

    val metadataStorage = FileMetadataStorage.load(yggConfig.dataDir, yggConfig.archiveDir, FilesystemFileOps).unsafePerformIO

    val shardActors @ ShardActors(ingestSupervisor, metadataActor, projectionsActor, metadataSync) = 
      actors.initShardActors(metadataStorage, extAccountManager)

    new ManagedQueryExecutorFactory 
        with PerAccountThreadPoolModule  
        with ActorStorageModule
        with ActorProjectionModule { self =>

      type YggConfig = JDBMQueryExecutorConfig
      val clock = blueeyes.util.Clock.System

      protected lazy val queryLogger = LoggerFactory.getLogger("com.precog.shard.ShardQueryExecutor")
      
      val actorSystem = ActorSystem("jdbmExecutorActorSystem")
      val jobActorSystem = ActorSystem("jobPollingActorSystem")
      val defaultAsyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

      val apiKeyManager = extAccessControl
      val accountManager = extAccountManager
      val jobManager = extJobManager
      val accessControl = extAccessControl

      class Storage extends ActorStorageLike(actorSystem, ingestSupervisor, metadataActor)

      val storage = new Storage

      val rawProjectionModule = new JDBMProjectionModule {
        object Projection extends JDBMProjectionCompanion {
          val fileOps = FilesystemFileOps

          def ensureBaseDir(descriptor: ProjectionDescriptor) = metadataStorage.ensureDescriptorRoot(descriptor)
          def findBaseDir(descriptor: ProjectionDescriptor) = metadataStorage.findDescriptorRoot(descriptor)
          def archiveDir(descriptor: ProjectionDescriptor) = metadataStorage.findArchiveRoot(descriptor)
        }
      }

      def asyncExecutorFor(apiKey: APIKey): Future[Validation[String, QueryExecutor[Future, JobId]]] = {
        implicit val futureMonad = new blueeyes.bkka.FutureMonad(defaultAsyncContext)
        (for {
          executionContext0 <- getAccountExecutionContext(apiKey)
        } yield {
          new AsyncQueryExecutor {
            val executionContext: ExecutionContext = executionContext0
          }
        }).validation
      }

      def syncExecutorFor(apiKey: APIKey): Future[Validation[String, QueryExecutor[Future, (Option[JobId], StreamT[Future, CharBuffer])]]] = {
        implicit val futureMonad = new blueeyes.bkka.FutureMonad(defaultAsyncContext)
        (for {
          executionContext0 <- getAccountExecutionContext(apiKey)
        } yield {
          new SyncQueryExecutor {
            val executionContext: ExecutionContext = executionContext0
          }
        }).validation
      }

      protected def executor(implicit shardQueryMonad: ShardQueryMonad): QueryExecutor[ShardQuery, StreamT[ShardQuery, CharBuffer]] = {
        new ShardQueryExecutor[ShardQuery] with JDBMColumnarTableModule[ShardQuery] {
          type YggConfig = JDBMQueryExecutorConfig
          type Key = Array[Byte]
          type Storage = StorageLike[ShardQuery]

          implicit val M = shardQueryMonad

          trait TableCompanion extends JDBMColumnarTableCompanion {
            import scalaz.std.anyVal._
            implicit val geq: scalaz.Equal[Int] = scalaz.Equal[Int]
          }

          object Table extends TableCompanion

          val yggConfig = self.yggConfig
          val storage = self.storage.liftM[JobQueryT](shardQueryMonad, shardQueryMonad.M)
          val report = errorReport[instructions.Line](shardQueryMonad, implicitly)

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
      }

      def shutdown() = ShardActors.stop(actorSystem, shardActors).onComplete { _ =>
        queryLogger.info("Actor ecossytem shutdown complete.")
      }
    }
  }
}
