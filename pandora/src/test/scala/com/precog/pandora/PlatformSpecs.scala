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
import yggdrasil.memoization._
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

  implicit val M = blueeyes.bkka.AkkaTypeClasses.futureApplicative(asyncContext)
  implicit val coM = new Copointed[Future] {
    def map[A, B](m: Future[A])(f: A => B) = m map f
    def copoint[A](f: Future[A]) = Await.result(f, yggConfig.maxEvalDuration)
  }

  class Storage extends SystemActorStorageLike(FileMetadataStorage.load(yggConfig.dataDir, FilesystemFileOps).unsafePerformIO) {
    val accessControl = new UnlimitedAccessControl[Future]()
  }

  val storage = new Storage

  object Projection extends JDBMProjectionCompanion {
    val fileOps = FilesystemFileOps
    def baseDir(descriptor: ProjectionDescriptor) = sys.error("todo")
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
