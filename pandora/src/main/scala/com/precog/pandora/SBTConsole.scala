package com.precog
package pandora

import common.kafka._
import common.security._

import daze._

import pandora._

import com.precog.common.Path
import com.precog.common.accounts._
import com.precog.common.jobs._
import com.precog.niflheim._
import com.precog.yggdrasil.vfs._
import com.precog.yggdrasil.scheduling._

import quirrel._
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import yggdrasil._
import yggdrasil.actor._
import yggdrasil.metadata._
import yggdrasil.nihdb._
import yggdrasil.serialization._
import yggdrasil.table._
import yggdrasil.util._

import com.precog.util.FilesystemFileOps

import akka.actor.ActorSystem
import akka.actor.Props
import akka.dispatch._
import akka.pattern.gracefulStop
import akka.util.{Duration, Timeout}

import blueeyes.bkka._
import blueeyes.util.Clock

import com.codecommit.gll.LineStream

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

import scalaz._
import scalaz.effect.IO
import scalaz.syntax.comonad._

import java.io.File

trait PlatformConfig extends BaseConfig
    with IdSourceConfig
    with EvaluatorConfig
    with StandaloneShardSystemConfig
    with ColumnarTableModuleConfig
    with BlockStoreColumnarTableModuleConfig

trait SBTConsolePlatform extends muspelheim.ParseEvalStack[Future]
    with SecureVFSModule[Future, Slice]
    with ActorVFSModule
    with IdSourceScannerModule
    with NIHDBColumnarTableModule
    with StandaloneActorProjectionSystem
    with LongIdMemoryDatasetConsumer[Future] { self =>

  type YggConfig = PlatformConfig

  trait TableCompanion extends NIHDBColumnarTableCompanion

  object Table extends TableCompanion
}

object SBTConsole {
  val controlTimeout = Duration(30, "seconds")

  val platform = new SBTConsolePlatform { console =>
    implicit val actorSystem = ActorSystem("sbtConsoleActorSystem")
    implicit val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
    implicit val M: Monad[Future] with Comonad[Future] = new UnsafeFutureComonad(asyncContext, yggConfig.maxEvalDuration)

    val yggConfig = new PlatformConfig {
      val config = Configuration parse {
        Option(System.getProperty("precog.storage.root")) map { "precog.storage.root = " + _ } getOrElse { "" }
      }

      val sortWorkDir = scratchDir
      val memoizationBufferSize = sortBufferSize
      val memoizationWorkDir = scratchDir

      val storageTimeout = Timeout(Duration(120, "seconds"))
      val flatMapTimeout = storageTimeout.duration
      val maxEvalDuration = storageTimeout.duration
      val clock = blueeyes.util.Clock.System
      val ingestConfig = None

      val maxSliceSize = 20000
      val cookThreshold = maxSliceSize
      val smallSliceSize = 8

      //TODO: Get a producer ID
      val idSource = new FreshAtomicIdSource
    }

    val accountFinder = new StaticAccountFinder[Future]("", "")
    val rawAPIKeyFinder = new InMemoryAPIKeyManager[Future](Clock.System)
    val accessControl = new DirectAPIKeyFinder(rawAPIKeyFinder)
    val permissionsFinder = new PermissionsFinder(accessControl, accountFinder, new org.joda.time.Instant())

    val rootAPIKey = rawAPIKeyFinder.rootAPIKey.copoint

    val storageTimeout = yggConfig.storageTimeout

    val masterChef = actorSystem.actorOf(Props(Chef(VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)), VersionedSegmentFormat(Map(1 -> V1SegmentFormat)))))

    val resourceBuilder = new ResourceBuilder(actorSystem, yggConfig.clock, masterChef, yggConfig.cookThreshold, yggConfig.storageTimeout)
    val projectionsActor = actorSystem.actorOf(Props(new PathRoutingActor(yggConfig.dataDir, yggConfig.storageTimeout.duration, yggConfig.clock)))

    val jobManager = new InMemoryJobManager[Future]
    val actorVFS = new ActorVFS(projectionsActor, yggConfig.storageTimeout, yggConfig.storageTimeout)
    val vfs = new SecureVFS(actorVFS, permissionsFinder, jobManager, NoopScheduler[Future], Clock.System)

    def Evaluator[N[+_]](N0: Monad[N])(implicit mn: Future ~> N, nm: N ~> Future): EvaluatorLike[N] =
      new Evaluator[N](N0) with IdSourceScannerModule {
        type YggConfig = PlatformConfig
        val yggConfig = console.yggConfig
        val report = LoggingQueryLogger[N](N0)
      }

    def eval(str: String): Set[SValue] = evalE(str)  match {
      case Success(results) => results.map(_._2)
      case Failure(t) => throw t
    }

    def evalE(str: String) = {
      val dag = produceDAG(str)
      consumeEval(rootAPIKey, dag, Path.Root)
    }

    def produceDAG(str: String) = {
      val forest = compile(str)
      val validForest = forest filter { _.errors.isEmpty }

      if (validForest.isEmpty) {
        val strs = forest map { tree =>
          tree.errors map showError mkString ("Set(\"", "\", \"", "\")")
        }

        sys.error(strs mkString " | ")
      }

      if (validForest.size > 1) {
        sys.error("ambiguous parse (good luck!)")
      }

      val tree = validForest.head
      val Right(dag) = decorate(emit(tree))
      dag
    }

    def startup() {
      // start storage shard
    }

    def shutdown() {
      // stop storage shard
      Await.result(gracefulStop(projectionsActor, yggConfig.storageTimeout.duration), yggConfig.storageTimeout.duration)
      actorSystem.shutdown()
    }
  }
}

// vim: set ts=4 sw=4 et:
