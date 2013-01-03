package com.precog.pandora

import com.precog.common.Path
import com.precog.common.VectorCase
import com.precog.common.kafka._
import com.precog.common.security._

import com.precog.bytecode.JType

import com.precog.daze._

import com.precog.quirrel._
import com.precog.quirrel.emitter._
import com.precog.quirrel.parser._
import com.precog.quirrel.typer._

import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.jdbm3._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.serialization._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.table.jdbm3._
import com.precog.yggdrasil.util._
import com.precog.yggdrasil.test.YId

import com.precog.muspelheim._
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

trait JDBMPlatformSpecs extends ParseEvalStackSpecs[Future] 
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

  val metadataStorage = FileMetadataStorage.load(yggConfig.dataDir, yggConfig.archiveDir, FilesystemFileOps).unsafePerformIO

  val accountFinder = None

  class Storage extends SystemActorStorageLike {
    val accessControl = new UnrestrictedAccessControl[Future]
  }

  val storage = new Storage

  object Projection extends JDBMProjectionCompanion {
    val fileOps = FilesystemFileOps
    def baseDir(descriptor: ProjectionDescriptor): IO[Option[File]] =
      metadataStorage.findDescriptorRoot(descriptor, false)
    def archiveDir(descriptor: ProjectionDescriptor): IO[Option[File]] =
      metadataStorage.findArchiveRoot(descriptor)
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

