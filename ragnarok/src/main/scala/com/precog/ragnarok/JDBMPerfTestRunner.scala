package com.precog
package ragnarok

import com.precog.daze._
import com.precog.accounts.InMemoryAccountManager

import yggdrasil.{ ProjectionDescriptor, BaseConfig }
import yggdrasil.actor._
import yggdrasil.table.SliceColumnarTableModule
import yggdrasil.table.BlockStoreColumnarTableModuleConfig
import yggdrasil.metadata.FileMetadataStorage

import common.security._

import util.{ FileOps, FilesystemFileOps }

import java.io.File

import akka.actor.ActorSystem
import akka.dispatch.{ Future, Await, ExecutionContext }

import org.streum.configrity.Configuration

import blueeyes.bkka.AkkaTypeClasses

import scalaz._
import scalaz.effect._
import scalaz.std.anyVal._


trait StandalonePerfTestRunner[T] extends EvaluatingPerfTestRunner[Future, T]
  with SystemActorStorageModule
  with StandaloneShardSystemActorModule
  with BatchJsonStorageModule[Future] {

  type YggConfig <: StandalonePerfTestRunnerConfig 
  
  trait StandalonePerfTestRunnerConfig extends BaseConfig with EvaluatingPerfTestRunnerConfig with StandaloneShardSystemConfig

  class Storage extends SystemActorStorageLike(FileMetadataStorage.load(yggConfig.dataDir, yggConfig.archiveDir, FilesystemFileOps).unsafePerformIO) {
    val accessControl = new UnrestrictedAccessControl[Future]()
    val accountManager = new InMemoryAccountManager[Future]()
  }

  val storage = new Storage

  def startup() {
    Await.result(storage.start(), yggConfig.maxEvalDuration)
  }

  def shutdown() {
    Await.result(storage.stop(), yggConfig.maxEvalDuration)
  }
}

final class JDBMPerfTestRunner[T](val timer: Timer[T], val apiKey: APIKey, val optimize: Boolean,
      val actorSystem: ActorSystem, _rootDir: Option[File])(implicit val M: Monad[Future] with Copointed[Future])
    extends StandalonePerfTestRunner[T]
    with SliceColumnarTableModule[Future, Array[Byte]]
    with StandaloneShardSystemActorModule[Array[Byte], table.Slice] { self =>

  trait JDBMPerfTestRunnerConfig extends StandalonePerfTestRunnerConfig with JDBMProjectionModuleConfig
    with BlockStoreColumnarTableModuleConfig

  type YggConfig = JDBMPerfTestRunnerConfig
  object yggConfig extends YggConfig {
    val apiKey = self.apiKey
    val optimize = self.optimize
    val commandLineConfig = Configuration.parse(_rootDir map ("precog.storage.root = " + _) getOrElse "")
    override val config = (Configuration parse {
      Option(System.getProperty("precog.storage.root")) map ("precog.storage.root = " + _) getOrElse "" }) ++ commandLineConfig
    val ingestConfig = None
  }

  trait TableCompanion extends JDBMColumnarTableCompanion
  object Table extends TableCompanion {
    implicit val geq: scalaz.Equal[Int] = intInstance
  }

  yggConfig.dataDir.mkdirs()
  val fileMetadataStorage = FileMetadataStorage.load(yggConfig.dataDir, yggConfig.archiveDir, FilesystemFileOps).unsafePerformIO

  object Projection extends JDBMProjectionCompanion {
    def fileOps = FilesystemFileOps
    def ensureBaseDir(descriptor: ProjectionDescriptor): IO[File] = fileMetadataStorage.ensureDescriptorRoot(descriptor)
    def findBaseDir(descriptor: ProjectionDescriptor): Option[File] = fileMetadataStorage.findDescriptorRoot(descriptor)
    def archiveDir(descriptor: ProjectionDescriptor): IO[Option[File]] = fileMetadataStorage.findArchiveRoot(descriptor)
  }

  val report = LoggingQueryLogger[Future]
}
