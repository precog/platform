package com.precog
package pandora

import common.VectorCase
import common.kafka._
import common.security._

import daze._

import pandora._

import quirrel._
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import yggdrasil._
import yggdrasil.actor._
import yggdrasil.jdbm3._
import yggdrasil.metadata._
import yggdrasil.serialization._
import yggdrasil.table._
import muspelheim._

import com.precog.util.FilesystemFileOps

import org.specs2.mutable._
  
import akka.actor.ActorSystem
import akka.dispatch._
import akka.util.duration._

import java.io.File

import scalaz._
import scalaz.std.anyVal._
import scalaz.effect.IO

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

class PlatformSpecs 
    extends ParseEvalStackSpecs[Future] 
    with BlockStoreColumnarTableModule[Future] 
    with JDBMProjectionModule 
    with SystemActorStorageModule 
    with StandaloneShardSystemActorModule { platformSpecs =>

  class YggConfig extends ParseEvalStackSpecConfig with StandaloneShardSystemConfig 
  object yggConfig  extends YggConfig

  implicit val M: Monad[Future] with Copointed[Future] = new blueeyes.bkka.FutureMonad(asyncContext) with Copointed[Future] {
    def copoint[A](f: Future[A]) = Await.result(f, yggConfig.maxEvalDuration)
  }

  type TableCompanion = BlockStoreColumnarTableCompanion
  object Table extends BlockStoreColumnarTableCompanion {
    implicit val geq: scalaz.Equal[Int] = intInstance
  }

  val fileMetadataStorage = FileMetadataStorage.load(yggConfig.dataDir, yggConfig.archiveDir, FilesystemFileOps).unsafePerformIO

  class Storage extends SystemActorStorageLike(fileMetadataStorage) {
    val accessControl = new UnlimitedAccessControl[Future]()
  }

  val storage = new Storage

  object Projection extends JDBMProjectionCompanion {
    val fileOps = FilesystemFileOps
    def baseDir(descriptor: ProjectionDescriptor): File =
      fileMetadataStorage.findDescriptorRoot(descriptor, false).unsafePerformIO getOrElse sys.error("Cannot find base dir. for descriptor: " + descriptor)
    def archiveDir(descriptor: ProjectionDescriptor): File =
      fileMetadataStorage.findArchiveRoot(descriptor).unsafePerformIO getOrElse sys.error("Cannot find base dir. for descriptor: " + descriptor)
  }

  override def startup() {
    // start storage shard 
    Await.result(storage.start(), controlTimeout)
  }
  
  override def shutdown() {
    // stop storage shard
    Await.result(storage.stop(), controlTimeout)
    
    actorSystem.shutdown()
  }
}
