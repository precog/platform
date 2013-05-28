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

object NIHDBPlatformSpecsActor extends NIHDBPlatformSpecsActor(Option(System.getProperty("precog.storage.root")))

class NIHDBPlatformSpecsActor(rootPath: Option[String]) extends Logging {
  abstract class YggConfig
      extends EvaluatorConfig
      with StandaloneShardSystemConfig
      with ColumnarTableModuleConfig
      with BlockStoreColumnarTableModuleConfig {
    val config = Configuration parse {
      rootPath map { "precog.storage.root = " + _ } getOrElse { "" }
    }

    // None of this is used, but is required to satisfy the cake
    val cookThreshold = 10
    val maxSliceSize = cookThreshold
    val smallSliceSize = 2
    val ingestConfig = None
    val idSource = new FreshAtomicIdSource
    val clock = blueeyes.util.Clock.System
    val storageTimeout = Timeout(Duration(120, "seconds"))
  }

  object yggConfig extends YggConfig

  case class SystemState(projectionsActor: ActorRef, actorSystem: ActorSystem)

  private[this] var state: Option[SystemState] = None
  private[this] val users = new AtomicInteger

  def actorSystem = users.synchronized {
    state.map(_.actorSystem)
  }

  def actor = users.synchronized {
    users.getAndIncrement

    if (state.isEmpty) {
      logger.info("Allocating new projections actor in " + this.hashCode)
      state = {
        val actorSystem = ActorSystem("NIHDBPlatformSpecsActor")
        val storageTimeout = Timeout(300 * 1000)

        implicit val M: Monad[Future] with Comonad[Future] = new blueeyes.bkka.UnsafeFutureComonad(actorSystem.dispatcher, storageTimeout.duration)

        val accountFinder = new StaticAccountFinder[Future](ParseEvalStackSpecs.testAccount, ParseEvalStackSpecs.testAPIKey, Some("/"))
        val accessControl = new StaticAPIKeyFinder[Future](ParseEvalStackSpecs.testAPIKey)
        val permissionsFinder = new PermissionsFinder(accessControl, accountFinder, new org.joda.time.Instant())

        val masterChef = actorSystem.actorOf(Props(Chef(VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)), VersionedSegmentFormat(Map(1 -> V1SegmentFormat)))))

        val resourceBuilder = new DefaultResourceBuilder(actorSystem, yggConfig.clock, masterChef, yggConfig.cookThreshold, yggConfig.storageTimeout, permissionsFinder)
        val projectionsActor = actorSystem.actorOf(Props(new PathRoutingActor(yggConfig.dataDir, resourceBuilder, permissionsFinder, yggConfig.storageTimeout.duration, new InMemoryJobManager[Future], yggConfig.clock)))

        Some(SystemState(projectionsActor, actorSystem))
      }
    }

    state.get.projectionsActor
  }

  def release = users.synchronized {
    users.getAndDecrement

    // Allow for a grace period
    state.foreach { case SystemState(_, as) => as.scheduler.scheduleOnce(Duration(5, "seconds")) { checkUnused }}
  }

  def checkUnused = users.synchronized {
    logger.debug("Checking for unused projectionsActor. Count = " + users.get)
    if (users.get == 0) {
      state.foreach {
        case SystemState(projectionsActor, actorSystem) =>
          logger.info("Culling unused projections actor")
          Await.result(gracefulStop(projectionsActor, Duration(5, "minutes"))(actorSystem), Duration(3, "minutes"))
          actorSystem.shutdown()
      }
      state = None
    }
  }
}

trait NIHDBPlatformSpecs extends ParseEvalStackSpecs[Future]
    with LongIdMemoryDatasetConsumer[Future]
    with NIHDBColumnarTableModule
    with NIHDBStorageMetadataSource { self =>

  override def map(fs: => Fragments): Fragments = step { startup() } ^ fs ^ step { shutdown() }

  lazy val psLogger = LoggerFactory.getLogger("com.precog.pandora.PlatformSpecs")

  abstract class YggConfig extends ParseEvalStackSpecConfig
      with IdSourceConfig
      with EvaluatorConfig
      with StandaloneShardSystemConfig
      with ColumnarTableModuleConfig
      with BlockStoreColumnarTableModuleConfig {
    val cookThreshold = 10
    val ingestConfig = None
  }

  object yggConfig extends YggConfig

  implicit val M: Monad[Future] with Comonad[Future] = new blueeyes.bkka.UnsafeFutureComonad(asyncContext, yggConfig.maxEvalDuration)

  val accountFinder = None

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

  override val accessControl = new UnrestrictedAccessControl[Future]

  val storageTimeout = Timeout(300 * 1000)

  val projectionsActor = NIHDBPlatformSpecsActor.actor

  val report = new LoggingQueryLogger[Future, instructions.Line] with ExceptionQueryLogger[Future, instructions.Line] with TimingQueryLogger[Future, instructions.Line] {
    implicit def M = self.M
  }

  trait TableCompanion extends NIHDBColumnarTableCompanion

  object Table extends TableCompanion

  def startup() { }

  def shutdown() {
    NIHDBPlatformSpecsActor.release
  }
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
