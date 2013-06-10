package com.precog.pandora

import com.precog.common.Path
import com.precog.util.VectorCase
import com.precog.common.accounts._
import com.precog.common.jobs._
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
import com.precog.yggdrasil.nihdb._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.scheduling._
import com.precog.yggdrasil.serialization._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.util._
import com.precog.yggdrasil.vfs._
import com.precog.yggdrasil.test.YId

import com.precog.muspelheim._
import com.precog.niflheim._
import com.precog.util.FilesystemFileOps

import akka.actor.{ActorRef, ActorSystem}
import akka.actor.Props
import akka.dispatch._
import akka.pattern.gracefulStop
import akka.util.{Duration, Timeout}
import akka.util.duration._

import blueeyes.bkka._
import blueeyes.json._

import com.weiglewilczek.slf4s.Logging

import org.slf4j.LoggerFactory

import org.specs2.mutable._
import org.specs2.specification.Fragments

import scalaz._
import scalaz.std.anyVal._
import scalaz.syntax.monad._
import scalaz.syntax.comonad._
import scalaz.effect.IO

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

trait NIHDBPlatformSpecs extends ParseEvalStackSpecs[Future] {
  type TestStack = NIHDBTestStack
  val stack = NIHDBTestStack

  override def map(fs: => Fragments): Fragments = step { stack.startup() } ^ fs ^ step { stack.shutdown() }
}

object NIHDBTestStack extends NIHDBTestStack {
  def startup() { }

  def shutdown() { }
}

trait NIHDBTestActors extends ActorVFSModule with ActorPlatformSpecs with YggConfigComponent {
  trait NIHDBTestActorsConfig extends BaseConfig with BlockStoreColumnarTableModuleConfig {
    val cookThreshold = 10
    val storageTimeout = Timeout(300 * 1000)
    val quiescenceTimeout = Duration(300, "seconds")

    def clock: blueeyes.util.Clock
  }

  type YggConfig <: NIHDBTestActorsConfig
  implicit val M: Monad[Future] with Comonad[Future] = new blueeyes.bkka.UnsafeFutureComonad(executor, yggConfig.storageTimeout.duration)

  val accountFinder = new StaticAccountFinder[Future](TestStack.testAccount, TestStack.testAPIKey, Some("/"))
  val apiKeyFinder = new StaticAPIKeyFinder[Future](TestStack.testAPIKey)
  val permissionsFinder = new PermissionsFinder(apiKeyFinder, accountFinder, yggConfig.clock.instant())
  val jobManager = new InMemoryJobManager[Future]

  val masterChef = actorSystem.actorOf(Props(Chef(VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)), VersionedSegmentFormat(Map(1 -> V1SegmentFormat)))))
  val resourceBuilder = new ResourceBuilder(actorSystem, yggConfig.clock, masterChef, yggConfig.cookThreshold, yggConfig.storageTimeout)

  val projectionsActor = actorSystem.actorOf(Props(new PathRoutingActor(yggConfig.dataDir, yggConfig.storageTimeout.duration, yggConfig.quiescenceTimeout, 10, yggConfig.clock)))
}

trait NIHDBTestStack extends TestStackLike[Future]
    with SecureVFSModule[Future, Slice]
    with LongIdMemoryDatasetConsumer[Future]
    with NIHDBTestActors
    with VFSColumnarTableModule { self =>

  abstract class YggConfig extends ParseEvalStackSpecConfig
      with IdSourceConfig
      with EvaluatorConfig
      with StandaloneShardSystemConfig
      with ColumnarTableModuleConfig
      with BlockStoreColumnarTableModuleConfig 
      with NIHDBTestActorsConfig {
    val ingestConfig = None
  }

  object yggConfig extends YggConfig

  lazy val psLogger = LoggerFactory.getLogger("com.precog.pandora.PlatformSpecs")

  //val accountFinder = None

  def Evaluator[N[+_]](N0: Monad[N])(implicit mn: Future ~> N, nm: N ~> Future) =
    new Evaluator[N](N0)(mn,nm) {
      val report = new LoggingQueryLogger[N, instructions.Line] with ExceptionQueryLogger[N, instructions.Line] with TimingQueryLogger[N, instructions.Line] {
        val M = N0
      }
      class YggConfig extends EvaluatorConfig {
        val idSource = new FreshAtomicIdSource
        val maxSliceSize = 10
      }
      val yggConfig = new YggConfig
      def freshIdScanner = self.freshIdScanner
  }

  val actorVFS = new ActorVFS(projectionsActor, yggConfig.storageTimeout, yggConfig.storageTimeout)
  val vfs = new SecureVFS(actorVFS, permissionsFinder, jobManager, yggConfig.clock)

  val report = new LoggingQueryLogger[Future, instructions.Line] with ExceptionQueryLogger[Future, instructions.Line] with TimingQueryLogger[Future, instructions.Line] {
    implicit def M = self.M
  }

  trait TableCompanion extends VFSColumnarTableCompanion

  object Table extends TableCompanion
}

class NIHDBBasicValidationSpecs extends BasicValidationSpecs with NIHDBPlatformSpecs

class NIHDBHelloQuirrelSpecs extends HelloQuirrelSpecs with NIHDBPlatformSpecs

class NIHDBLogisticRegressionSpecs extends LogisticRegressionSpecs with NIHDBPlatformSpecs

class NIHDBLinearRegressionSpecs extends LinearRegressionSpecs with NIHDBPlatformSpecs

class NIHDNormalizationSpecs extends NormalizationSpecs with NIHDBPlatformSpecs

class NIHDBEnrichmentSpecs extends EnrichmentSpecs with NIHDBPlatformSpecs

class NIHDBClusteringSpecs extends ClusteringSpecs with NIHDBPlatformSpecs

class NIHDBRandomForestSpecs extends RandomForestSpecs with NIHDBPlatformSpecs

class NIHDBMiscStackSpecs extends MiscStackSpecs with NIHDBPlatformSpecs

class NIHDBNonObjectStackSpecs extends NonObjectStackSpecs with NIHDBPlatformSpecs

class NIHDBRankSpecs extends RankSpecs with NIHDBPlatformSpecs

class NIHDBRenderStackSpecs extends RenderStackSpecs with NIHDBPlatformSpecs

class NIHDBUndefinedLiteralSpecs extends UndefinedLiteralSpecs with NIHDBPlatformSpecs

class NIHDBRandomSpecs extends RandomStackSpecs with NIHDBPlatformSpecs
