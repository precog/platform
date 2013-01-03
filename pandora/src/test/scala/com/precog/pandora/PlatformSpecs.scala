package com.precog
package pandora

import accounts.InMemoryAccountManager

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
import yggdrasil.table.jdbm3._
import yggdrasil.util._
import yggdrasil.test.YId
import muspelheim._

import com.precog.bytecode.JType
import com.precog.common.Path
import com.precog.util.FilesystemFileOps

import org.specs2.mutable._
import org.specs2.specification.Fragments
  
import akka.actor.ActorSystem
import akka.dispatch._
import akka.util.duration._

import java.io.File

import blueeyes.json._

import org.slf4j.LoggerFactory

import scalaz._
import scalaz.std.anyVal._
import scalaz.syntax.monad._
import scalaz.syntax.copointed._
import scalaz.effect.IO

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

trait PlatformSpec extends ParseEvalStackSpecs[Future] 
    with JDBMColumnarTableModule[Future] 
    with SystemActorStorageModule 
    with StandaloneShardSystemActorModule 
    with JDBMProjectionModule {
      
  lazy val psLogger = LoggerFactory.getLogger("com.precog.pandora.PlatformSpecs")

  class YggConfig extends ParseEvalStackSpecConfig
      with StandaloneShardSystemConfig
      with IdSourceConfig
      with ColumnarTableModuleConfig
      with BlockStoreColumnarTableModuleConfig
      with JDBMProjectionModuleConfig
      
  object yggConfig  extends YggConfig

  override def map(fs: => Fragments): Fragments = step { startup() } ^ fs ^ step { shutdown() }
      
  implicit val M: Monad[Future] with Copointed[Future] = new blueeyes.bkka.FutureMonad(asyncContext) with Copointed[Future] {
    def copoint[A](f: Future[A]) = Await.result(f, yggConfig.maxEvalDuration)
  }

  val fileMetadataStorage = FileMetadataStorage.load(yggConfig.dataDir, yggConfig.archiveDir, FilesystemFileOps).unsafePerformIO

  class Storage extends SystemActorStorageLike(fileMetadataStorage) {
    val accessControl = new UnrestrictedAccessControl[Future]
    val accountManager = new InMemoryAccountManager[Future]()
  }

  val storage = new Storage

  object Projection extends JDBMProjectionCompanion {
    val fileOps = FilesystemFileOps
    def baseDir(descriptor: ProjectionDescriptor): IO[Option[File]] =
      fileMetadataStorage.findDescriptorRoot(descriptor, false)
    def archiveDir(descriptor: ProjectionDescriptor): IO[Option[File]] =
      fileMetadataStorage.findArchiveRoot(descriptor)
  }

  trait TableCompanion extends JDBMColumnarTableCompanion
  object Table extends TableCompanion {
    //override def apply(slices: StreamT[M, Slice]) = super.apply(slices map { s => if (s.size != 96) s else sys.error("Slice size seen as 96 for the first time.") })
    implicit val geq: scalaz.Equal[Int] = intInstance
  }

  def startup() {
    // start storage shard 
    Await.result(storage.start(), controlTimeout)
    psLogger.info("Test shard started")
  }
  
  def shutdown() {
    psLogger.info("Shutting down test shard")
    // stop storage shard
    Await.result(storage.stop(), controlTimeout)
    
    actorSystem.shutdown()
  }
}

