package com.precog
package ragnarok

import com.precog.daze._
import com.precog.accounts.InMemoryAccountManager

import yggdrasil.{ ProjectionDescriptor, BaseConfig }
import yggdrasil.actor._
import yggdrasil.jdbm3._
import yggdrasil.table._
import yggdrasil.metadata.FileMetadataStorage

import common.security._

import util.{ FileOps, FilesystemFileOps }

import java.io.File

import akka.actor.{ActorSystem, Props}
import akka.dispatch.{ Future, Await, ExecutionContext }
import akka.util.Duration

import org.streum.configrity.Configuration

import blueeyes.bkka._

import scalaz._
import scalaz.effect._
import scalaz.std.anyVal._

trait StandalonePerfTestRunnerConfig extends BaseConfig
    with EvaluatingPerfTestRunnerConfig
    with StandaloneShardSystemConfig
    with JDBMProjectionModuleConfig
    with ActorStorageModuleConfig
    with ActorProjectionModuleConfig {
  def ingestConfig = None
}


abstract class StandalonePerfTestRunner[T](testTimeout: Duration) extends EvaluatingPerfTestRunner[Future, T]
    with ActorStorageModule
    with ActorProjectionModule[Array[Byte], Slice]
    with StandaloneActorProjectionSystem
    with BatchJsonStorageModule[Future] { self =>

  type YggConfig <: StandalonePerfTestRunnerConfig

  val ms = FileMetadataStorage.load(yggConfig.dataDir, yggConfig.archiveDir, FilesystemFileOps).unsafePerformIO

  val rawProjectionModule = new JDBMProjectionModule {
    type YggConfig = StandalonePerfTestRunnerConfig
    val yggConfig = self.yggConfig
    val Projection = new ProjectionCompanion {
      def fileOps = FilesystemFileOps
      def ensureBaseDir(descriptor: ProjectionDescriptor): IO[File] = ms.ensureDescriptorRoot(descriptor)
      def findBaseDir(descriptor: ProjectionDescriptor): Option[File] = ms.findDescriptorRoot(descriptor)
      def archiveDir(descriptor: ProjectionDescriptor): IO[Option[File]] = ms.findArchiveRoot(descriptor)
    }
  }

  implicit val actorSystem = ActorSystem("StandalonePerfTestRunner")
  implicit val defaultAsyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit val M: Monad[Future] with Copointed[Future] = new blueeyes.bkka.FutureMonad(defaultAsyncContext) with Copointed[Future] {
    def copoint[A](p: Future[A]): A = Await.result(p, testTimeout)
  }

  val projectionsActor = actorSystem.actorOf(Props(new ProjectionsActor), "projections")
  val shardActors @ ShardActors(ingestSupervisor, metadataActor, metadataSync) =
    initShardActors(ms, new InMemoryAccountManager[Future](), projectionsActor)

  object Projection extends ProjectionCompanion(projectionsActor, yggConfig.metadataTimeout)

  val accessControl = new UnrestrictedAccessControl[Future]()
  class Storage extends ActorStorageLike(actorSystem, ingestSupervisor, metadataActor)

  val storage = new Storage

  def userMetadataView(apiKey: APIKey) = storage.userMetadataView(apiKey)

  def startup() {}

  def shutdown() {
    Await.result(Stoppable.stop(shardActors.stoppable), Duration(2, "minutes"))
    actorSystem.shutdown()
  }
}

final class JDBMPerfTestRunner[T](val timer: Timer[T], val apiKey: APIKey, val optimize: Boolean, _rootDir: Option[File], testTimeout: Duration = Duration(120, "seconds"))
    extends StandalonePerfTestRunner[T](testTimeout)
    with SliceColumnarTableModule[Future, Array[Byte]]
 { self =>

  trait JDBMPerfTestRunnerConfig 
      extends StandalonePerfTestRunnerConfig
      with JDBMProjectionModuleConfig
      with BlockStoreColumnarTableModuleConfig

  type YggConfig = JDBMPerfTestRunnerConfig
  object yggConfig extends JDBMPerfTestRunnerConfig {
    val apiKey = self.apiKey
    val optimize = self.optimize
    val commandLineConfig = Configuration.parse(_rootDir map ("precog.storage.root = " + _) getOrElse "")
    override val config = (Configuration parse {
      Option(System.getProperty("precog.storage.root")) map ("precog.storage.root = " + _) getOrElse "" }) ++ commandLineConfig
  }

  trait TableCompanion extends SliceColumnarTableCompanion
  object Table extends TableCompanion {
    implicit val geq: scalaz.Equal[Int] = intInstance
  }

  yggConfig.dataDir.mkdirs()
  val fileMetadataStorage = FileMetadataStorage.load(yggConfig.dataDir, yggConfig.archiveDir, FilesystemFileOps).unsafePerformIO

  val report = LoggingQueryLogger[Future]
}
