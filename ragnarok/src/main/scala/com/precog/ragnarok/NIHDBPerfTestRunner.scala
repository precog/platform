package com.precog.ragnarok

import com.precog.common.accounts._
import com.precog.common.jobs._
import com.precog.common.security._
import com.precog.daze._
import com.precog.niflheim._
import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.nihdb._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.util._
import com.precog.yggdrasil.vfs._
import com.precog.util.{ FileOps, FilesystemFileOps }

import java.io.File

import akka.actor.{ActorSystem, Props}
import akka.routing.RoundRobinRouter
import akka.dispatch.{ Future, Await, ExecutionContext }
import akka.util.{ Timeout, Duration }
import akka.pattern.gracefulStop

import org.streum.configrity.Configuration

import blueeyes.bkka._

import scalaz._
import scalaz.effect._
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.syntax.traverse._

final class NIHDBPerfTestRunner[T](val timer: Timer[T], val apiKey: APIKey, val optimize: Boolean, _rootDir: Option[File], testTimeout: Duration = Duration(120, "seconds"))
    extends EvaluatingPerfTestRunner[Future, T]
    with NIHDBColumnarTableModule
    with NIHDBStorageMetadataSource
    with NIHDBIngestSupport { self =>
    // with StandaloneActorProjectionSystem
    // with SliceColumnarTableModule[Future, Array[Byte]] { self =>

  trait NIHDBPerfTestRunnerConfig 
      extends BaseConfig
      with EvaluatingPerfTestRunnerConfig
      with BlockStoreColumnarTableModuleConfig
      with EvaluatorConfig

  implicit val actorSystem = ActorSystem("NIHDBPerfTestRunner")
  implicit val M = new UnsafeFutureComonad(actorSystem.dispatcher, testTimeout)

  type YggConfig = NIHDBPerfTestRunnerConfig
  object yggConfig extends NIHDBPerfTestRunnerConfig {
    val ingestConfig = None
    val apiKey = self.apiKey
    val optimize = self.optimize
    val commandLineConfig = Configuration.parse(_rootDir map ("precog.storage.root = " + _) getOrElse "")
    override val config = (Configuration parse {
      Option(System.getProperty("precog.storage.root")) map ("precog.storage.root = " + _) getOrElse "" }) ++ commandLineConfig
    val cookThreshold = 10000
    val clock = blueeyes.util.Clock.System
    val storageTimeout = Timeout(Duration(120, "seconds"))
  }

  yggConfig.dataDir.mkdirs()

  trait TableCompanion extends NIHDBColumnarTableCompanion
  object Table extends TableCompanion

  val accountFinder = new StaticAccountFinder[Future]("", "")
  val apiKeyManager = new InMemoryAPIKeyManager[Future](blueeyes.util.Clock.System)
  val accessControl = new DirectAPIKeyFinder(apiKeyManager)
  val permissionsFinder = new PermissionsFinder(accessControl, accountFinder, new org.joda.time.Instant())

  val storageTimeout = Timeout(testTimeout)

  private def makeChef = Chef(
    VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)),
    VersionedSegmentFormat(Map(1 -> V1SegmentFormat)))

  val chefs = (1 to 4).map { _ => actorSystem.actorOf(Props(makeChef)) }
  val masterChef = actorSystem.actorOf(Props[Chef].withRouter(RoundRobinRouter(chefs)))
  val resourceBuilder = new DefaultResourceBuilder(actorSystem, yggConfig.clock, masterChef, yggConfig.cookThreshold, yggConfig.storageTimeout, permissionsFinder)
  val projectionsActor = actorSystem.actorOf(Props(new PathRoutingActor(yggConfig.dataDir, resourceBuilder, permissionsFinder, yggConfig.storageTimeout.duration, new InMemoryJobManager[Future], yggConfig.clock)))

  def Evaluator[N[+_]](N0: Monad[N])(implicit mn: Future ~> N, nm: N ~> Future): EvaluatorLike[N] = {
    new Evaluator[N](N0) with IdSourceScannerModule {
      type YggConfig = self.YggConfig
      val yggConfig = self.yggConfig
      val report = LoggingQueryLogger[N](N0)
    }
  }

  def startup() {}

  def shutdown() {
    val stopWait = yggConfig.storageTimeout.duration
    Await.result(chefs.toList.traverse(gracefulStop(_, stopWait)), stopWait)

    Await.result(gracefulStop(masterChef, stopWait), stopWait)
    logger.info("Completed chef shutdown")

    logger.info("Waiting for shard shutdown")
    Await.result(gracefulStop(projectionsActor, stopWait), stopWait)
    actorSystem.shutdown()
  }
}
