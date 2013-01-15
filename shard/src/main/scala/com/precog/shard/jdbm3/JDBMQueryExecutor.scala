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

trait JDBMQueryExecutorConfig extends BaseJDBMQueryExecutorConfig with ProductionShardSystemConfig with SystemActorStorageConfig {
  def ingestFailureLogRoot: File
}

trait JDBMQueryExecutorComponent {
  import blueeyes.json.serialization.Extractor

  private def wrapConfig(wrappedConfig: Configuration) = {
    new JDBMQueryExecutorConfig {
      val config = wrappedConfig 
      val sortWorkDir = scratchDir
      val memoizationBufferSize = sortBufferSize
      val memoizationWorkDir = scratchDir

      val clock = blueeyes.util.Clock.System
      val maxSliceSize = config[Int]("jdbm.max_slice_size", 10000)
      val ingestFailureLogRoot = new File(config[String]("ingest.failure_log_root"))

      //TODO: Get a producer ID
      val idSource = new FreshAtomicIdSource
    }
  }

  def queryExecutorFactoryFactory(config: Configuration,
      extAccessControl: APIKeyManager[Future],
      extAccountManager: BasicAccountManager[Future],
      extJobManager: JobManager[Future]): AsyncQueryExecutorFactory = {
    new JDBMQueryExecutorFactory
        with JDBMProjectionModule
        with ProductionShardSystemActorModule
        with SystemActorStorageModule { self =>

      type YggConfig = JDBMQueryExecutorConfig
      val yggConfig = wrapConfig(config)
      val clock = blueeyes.util.Clock.System

      protected lazy val queryLogger = LoggerFactory.getLogger("com.precog.shard.ShardQueryExecutor")
      
      val actorSystem = ActorSystem("jdbmExecutorActorSystem")
      val defaultAsyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

      val accountManager = extAccountManager
      val jobManager = extJobManager

      class Storage extends SystemActorStorageLike(FileMetadataStorage.load(yggConfig.dataDir, yggConfig.archiveDir, FilesystemFileOps).unsafePerformIO) {
        val accessControl = extAccessControl
        val accountManager = extAccountManager
      }

      val storage = new Storage
      def storageMetadataSource = storage

      val apiKeyManager = extAccessControl
      def ingestFailureLog(checkpoint: YggCheckpoint): IngestFailureLog = FilesystemIngestFailureLog(yggConfig.ingestFailureLogRoot, checkpoint)

      object Projection extends JDBMProjectionCompanion {
        private lazy val logger = LoggerFactory.getLogger("com.precog.shard.yggdrasil.JDBMQueryExecutor.Projection")

        private implicit val askTimeout = yggConfig.projectionRetrievalTimeout

        val fileOps = FilesystemFileOps

        def ensureBaseDir(descriptor: ProjectionDescriptor) = {
          logger.trace("Ensuring base dir for " + descriptor)
          val base = (storage.shardSystemActor ? InitDescriptorRoot(descriptor)).mapTo[File]
          IO { Await.result(base, yggConfig.maxEvalDuration) }
        }

        def findBaseDir(descriptor: ProjectionDescriptor) = {
          logger.trace("Finding base dir for " + descriptor)
          val base = (storage.shardSystemActor ? FindDescriptorRoot(descriptor)).mapTo[Option[File]]
          Await.result(base, yggConfig.maxEvalDuration)
        }

        def archiveDir(descriptor: ProjectionDescriptor) = {
          logger.trace("Finding archive dir for " + descriptor)
          val archive = (storage.shardSystemActor ? FindDescriptorArchive(descriptor)).mapTo[Option[File]]
          IO { Await.result(archive, yggConfig.maxEvalDuration) }
        }
      }

      trait JDBMShardQueryExecutor 
          extends ShardQueryExecutor[ShardQuery]
          with JDBMColumnarTableModule[ShardQuery] {
        type YggConfig = JDBMQueryExecutorConfig
        type Key = Array[Byte]
        type Projection = JDBMProjection
        type Storage = StorageLike[ShardQuery, JDBMProjection]
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

      def executorFor(apiKey: APIKey): Future[Validation[String, QueryExecutor[Future, StreamT[Future, CharBuffer]]]] = {
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
        new JDBMShardQueryExecutor {
          implicit val M = shardQueryMonad

          trait TableCompanion extends JDBMColumnarTableCompanion {
            import scalaz.std.anyVal._
            implicit val geq: scalaz.Equal[Int] = scalaz.Equal[Int]
          }

          object Table extends TableCompanion

          val yggConfig = self.yggConfig
          val storage = self.storage.liftM[JobQueryT](shardQueryMonad, shardQueryMonad.M)
        }
      }

      def startup() = storage.start.onComplete {
        case Left(error) => queryLogger.error("Startup of actor ecosystem failed!", error)
        case Right(_) => queryLogger.info("Actor ecosystem started.")
      }

      def shutdown() = storage.stop.onComplete {
        case Left(error) => queryLogger.error("An error was encountered in actor ecosystem shutdown!", error)
        case Right(_) => queryLogger.info("Actor ecossytem shutdown complete.")
      }
    }
  }
}

trait JDBMQueryExecutorFactory
    extends QueryExecutorFactory[Future, StreamT[Future, CharBuffer]]
    with StorageModule[Future]
    with PerAccountThreadPoolModule
    with ManagedQueryModule
    with AsyncQueryExecutorFactory { self =>

  type YggConfig <: BaseJDBMQueryExecutorConfig

  def status(): Future[Validation[String, JValue]] = Future(Failure("Status not supported yet."))

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

