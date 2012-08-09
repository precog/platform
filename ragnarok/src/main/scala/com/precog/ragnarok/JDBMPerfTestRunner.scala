package com.precog
package ragnarok

import yggdrasil.ProjectionDescriptor
import yggdrasil.jdbm3._
import yggdrasil.actor._
import yggdrasil.table.BlockStoreColumnarTableModule
import yggdrasil.metadata.FileMetadataStorage

import common.security.UnlimitedAccessControl

import util.{ FileOps, FilesystemFileOps }

import akka.actor.ActorSystem
import akka.dispatch.{ Future, Await }

import org.streum.configrity.Configuration

import scalaz._


trait StandalonePerfTestRunner[T] extends EvaluatingPerfTestRunner[Future, T]
  with SystemActorStorageModule
  with StandaloneShardSystemActorModule {

  // implicit def M: Monad[Future]

  type YggConfig <: StandalonePerfTestRunnerConfig 
  
  trait StandalonePerfTestRunnerConfig extends EvaluatingPerfTestRunnerConfig with StandaloneShardSystemConfig

  class Storage extends SystemActorStorageLike(FileMetadataStorage.load(yggConfig.dataDir, FilesystemFileOps).unsafePerformIO) {
    val accessControl = new UnlimitedAccessControl[Future]()
  }

  val storage = new Storage

  def startup() {
    Await.result(storage.start(), yggConfig.maxEvalDuration)
  }

  def shutdown() {
    Await.result(storage.stop(), yggConfig.maxEvalDuration)
  }
}


final class JDBMPerfTestRunner[T](val timer: Timer[T], val userUID: String, val optimize: Boolean,
      val actorSystem: ActorSystem)(implicit val M: Monad[Future])
    extends StandalonePerfTestRunner[T]
    with BlockStoreColumnarTableModule[Future]
    with JDBMProjectionModule { self =>

  type YggConfig = StandalonePerfTestRunnerConfig 
  object yggConfig extends YggConfig {
    val userUID = self.userUID
    val optimize = self.optimize
    override val config = Configuration parse {
      Option(System.getProperty("precog.storage.root")) map { "precog.storage.root = " + _ } getOrElse { "" }
    }
  }

  object Projection extends JDBMProjectionCompanion {
    val fileOps = FilesystemFileOps
    def baseDir(descriptor: ProjectionDescriptor) = sys.error("todo")
  }
}
