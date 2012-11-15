package com.precog
package ragnarok

import yggdrasil.{ ProjectionDescriptor, BaseConfig }
import yggdrasil.jdbm3._
import yggdrasil.actor._
import yggdrasil.table.BlockStoreColumnarTableModule
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
      val actorSystem: ActorSystem, _rootDir: Option[File])(implicit val M: Monad[Future], val coM: Copointed[Future])
    extends StandalonePerfTestRunner[T]
    with BlockStoreColumnarTableModule[Future]
    with SystemActorStorageModule
    with JDBMProjectionModule
    with StandaloneShardSystemActorModule { self =>

  trait JDBMPerfTestRunnerConfig extends StandalonePerfTestRunnerConfig with JDBMProjectionModuleConfig
    with BlockStoreColumnarTableModuleConfig

  type YggConfig = JDBMPerfTestRunnerConfig
  object yggConfig extends YggConfig {
    val apiKey = self.apiKey
    val optimize = self.optimize
    val commandLineConfig = Configuration.parse(_rootDir map ("precog.storage.root = " + _) getOrElse "")
    override val config = (Configuration parse {
      Option(System.getProperty("precog.storage.root")) map ("precog.storage.root = " + _) getOrElse "" }) ++ commandLineConfig
  }

  type TableCompanion = BlockStoreColumnarTableCompanion
  object Table extends BlockStoreColumnarTableCompanion {
    implicit val geq: scalaz.Equal[Int] = intInstance
  }

  yggConfig.dataDir.mkdirs()
  val fileMetadataStorage = FileMetadataStorage.load(yggConfig.dataDir, yggConfig.archiveDir, FilesystemFileOps).unsafePerformIO

  object Projection extends JDBMProjectionCompanion {
    val fileOps = FilesystemFileOps

    def baseDir(descriptor: ProjectionDescriptor): IO[Option[File]] =
      fileMetadataStorage.findDescriptorRoot(descriptor, true)
    // map (_ getOrElse sys.error("Cannot find base dir. for descriptor: " + descriptor))

    def archiveDir(descriptor: ProjectionDescriptor): IO[Option[File]] =
      fileMetadataStorage.findArchiveRoot(descriptor)
    //map (_ getOrElse sys.error("Cannot find base dir. for descriptor: " + descriptor))
  }
}
