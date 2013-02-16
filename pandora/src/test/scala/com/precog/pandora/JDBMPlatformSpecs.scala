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
import yggdrasil.util._
import yggdrasil.test.YId
import muspelheim._

import com.precog.bytecode.JType
import com.precog.common.Path
import com.precog.util.FilesystemFileOps

import org.specs2.mutable._
import org.specs2.specification.Fragments
  
import akka.actor.ActorSystem
import akka.actor.Props
import akka.dispatch._
import akka.util.Duration
import akka.util.duration._

import java.io.File

import blueeyes.bkka._
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
    with LongIdMemoryDatasetConsumer[Future]
    with SliceColumnarTableModule[Future, Array[Byte]] 
    with ActorStorageModule
    with ActorProjectionModule[Array[Byte], Slice]
    with StandaloneActorProjectionSystem { self =>
      
  override def map(fs: => Fragments): Fragments = step { startup() } ^ fs ^ step { shutdown() }
      
  lazy val psLogger = LoggerFactory.getLogger("com.precog.pandora.PlatformSpecs")

  abstract class YggConfig extends ParseEvalStackSpecConfig
      with IdSourceConfig
      with EvaluatorConfig
      with StandaloneShardSystemConfig
      with ColumnarTableModuleConfig
      with BlockStoreColumnarTableModuleConfig
      with JDBMProjectionModuleConfig
      with ActorStorageModuleConfig
      with ActorProjectionModuleConfig
      
  object yggConfig extends YggConfig {
    val ingestConfig = None
  }

  implicit val M: Monad[Future] with Copointed[Future] = new blueeyes.bkka.FutureMonad(asyncContext) with Copointed[Future] {
    def copoint[A](f: Future[A]) = Await.result(f, yggConfig.maxEvalDuration)
  }

  def Evaluator[N[+_]](N0: Monad[N])(implicit mn: Future ~> N, nm: N ~> Future) = 
    new Evaluator[N](N0)(mn,nm) with IdSourceScannerModule {
      val report = new LoggingQueryLogger[N, instructions.Line] with ExceptionQueryLogger[N, instructions.Line] with TimingQueryLogger[N, instructions.Line] {
        val M = N0
      }
      class YggConfig extends EvaluatorConfig {
        val idSource = new FreshAtomicIdSource
        val maxSliceSize = 10
      }
      val yggConfig = new YggConfig
  }

  val rawProjectionModule = new JDBMProjectionModule {
    type YggConfig = self.YggConfig
    val yggConfig = self.yggConfig
    val Projection = new ProjectionCompanion {
      def fileOps = FilesystemFileOps
      def ensureBaseDir(descriptor: ProjectionDescriptor): IO[File] = fileMetadataStorage.ensureDescriptorRoot(descriptor)
      def findBaseDir(descriptor: ProjectionDescriptor): Option[File] = fileMetadataStorage.findDescriptorRoot(descriptor)
      def archiveDir(descriptor: ProjectionDescriptor): IO[Option[File]] = fileMetadataStorage.findArchiveRoot(descriptor)
    }
  }

  val fileMetadataStorage = FileMetadataStorage.load(yggConfig.dataDir, yggConfig.archiveDir, FilesystemFileOps).unsafePerformIO
  val projectionsActor = actorSystem.actorOf(Props(new ProjectionsActor), "projections")
  val shardActors @ ShardActors(ingestSupervisor, metadataActor, metadataSync) =
    initShardActors(fileMetadataStorage, new InMemoryAccountManager[Future](), projectionsActor)

  val accessControl = new UnrestrictedAccessControl[Future]
  class Storage extends ActorStorageLike(actorSystem, ingestSupervisor, metadataActor)
  val storage = new Storage

  def userMetadataView(apiKey: APIKey) = storage.userMetadataView(apiKey)

  val report = new LoggingQueryLogger[Future, instructions.Line] with ExceptionQueryLogger[Future, instructions.Line] with TimingQueryLogger[Future, instructions.Line] {
    implicit def M = self.M
  }

  object Projection extends ProjectionCompanion(projectionsActor, yggConfig.metadataTimeout)

  trait TableCompanion extends SliceColumnarTableCompanion
  object Table extends TableCompanion {
    implicit val geq: scalaz.Equal[Int] = intInstance
  }

  def startup() { }
  
  def shutdown() {
    Await.result(Stoppable.stop(shardActors.stoppable), Duration(3, "minutes"))
    actorSystem.shutdown()
  }
}

class JDBMBasicValidationSpecs extends BasicValidationSpecs with JDBMPlatformSpecs

class JDBMHelloQuirrelSpecs extends HelloQuirrelSpecs with JDBMPlatformSpecs

class JDBMLogisticRegressionSpecs extends LogisticRegressionSpecs with JDBMPlatformSpecs

class JDBMLinearRegressionSpecs extends LinearRegressionSpecs with JDBMPlatformSpecs

class JDBMClusteringSpecs extends ClusteringSpecs with JDBMPlatformSpecs

class JDBMMiscStackSpecs extends MiscStackSpecs with JDBMPlatformSpecs

class JDBMNonObjectStackSpecs extends NonObjectStackSpecs with JDBMPlatformSpecs

class JDBMRankSpecs extends RankSpecs with JDBMPlatformSpecs

class JDBMRenderStackSpecs extends RenderStackSpecs with JDBMPlatformSpecs

class JDBMUndefinedLiteralSpecs extends UndefinedLiteralSpecs with JDBMPlatformSpecs
