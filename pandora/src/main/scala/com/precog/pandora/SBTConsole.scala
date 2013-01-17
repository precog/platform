package com.precog
package pandora

import common.kafka._
import common.security._

import daze._

import pandora._

import com.precog.accounts.InMemoryAccountManager

import com.precog.common.Path

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

import com.precog.util.FilesystemFileOps

import akka.actor.ActorSystem
import akka.dispatch._
import akka.util.Duration

import com.codecommit.gll.LineStream

object SBTConsole {
  
  trait Platform 
      extends muspelheim.ParseEvalStack[Future] 
      with IdSourceScannerModule[Future] 
      with PrettyPrinter
      with LongIdMemoryDatasetConsumer[Future]
      with JDBMColumnarTableModule[Future]
      with JDBMProjectionModule
      with SystemActorStorageModule
      with StandaloneShardSystemActorModule {

    trait YggConfig
        extends BaseConfig
        with IdSourceConfig
        with ColumnarTableModuleConfig
        with StandaloneShardSystemConfig
        with JDBMProjectionModuleConfig
        with BlockStoreColumnarTableModuleConfig

    trait TableCompanion extends JDBMColumnarTableCompanion {
      import scalaz.std.anyVal._
      implicit val geq: scalaz.Equal[Int] = scalaz.Equal[Int]
    }

    object Table extends TableCompanion
  }

  val controlTimeout = Duration(30, "seconds")

  val platform = new Platform { console =>
    import akka.dispatch.Await
    import akka.util.Duration
    import scalaz._

    import java.io.File

    import scalaz.effect.IO
    
    import org.streum.configrity.Configuration
    import org.streum.configrity.io.BlockFormat

    implicit val actorSystem = ActorSystem("sbtConsoleActorSystem")
    implicit val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

    object yggConfig extends YggConfig {
      val config = Configuration parse {
        Option(System.getProperty("precog.storage.root")) map { "precog.storage.root = " + _ } getOrElse { "" }
      }

      val sortWorkDir = scratchDir
      val memoizationBufferSize = sortBufferSize
      val memoizationWorkDir = scratchDir

      val flatMapTimeout = controlTimeout
      val projectionRetrievalTimeout = akka.util.Timeout(controlTimeout)
      val maxEvalDuration = controlTimeout
      val clock = blueeyes.util.Clock.System
      val ingestConfig = None
      
      val maxSliceSize = 10000
      val smallSliceSize = 8

      //TODO: Get a producer ID
      val idSource = new FreshAtomicIdSource
    }

    implicit val M: Monad[Future] with Copointed[Future] = new blueeyes.bkka.FutureMonad(asyncContext) with Copointed[Future] {
      def copoint[A](f: Future[A]) = Await.result(f, yggConfig.maxEvalDuration)
    }

    class Storage extends SystemActorStorageLike(FileMetadataStorage.load(yggConfig.dataDir, yggConfig.archiveDir, FilesystemFileOps).unsafePerformIO) {
      val accessControl = new UnrestrictedAccessControl[Future]()
      val accountManager = new InMemoryAccountManager[Future]()
    }

    val storage = new Storage

    val report = LoggingQueryLogger[Future]

    object Projection extends JDBMProjectionCompanion {
      val fileOps = FilesystemFileOps
      def ensureBaseDir(descriptor: ProjectionDescriptor) = sys.error("todo")
      def findBaseDir(descriptor: ProjectionDescriptor) = sys.error("todo")
      def archiveDir(descriptor: ProjectionDescriptor) = sys.error("todo")
    }

    def eval(str: String): Set[SValue] = evalE(str)  match {
      case Success(results) => results.map(_._2)
      case Failure(t) => throw t
    }

    def evalE(str: String) = {
      val dag = produceDAG(str)
      consumeEval("dummyAPIKey", dag, Path.Root) 
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

    def printDAG(str: String) = {
      val dag = produceDAG(str)
      prettyPrint(dag)
    }

    def startup() {
      // start storage shard 
      Await.result(storage.start(), controlTimeout)
    }
    
    def shutdown() {
      // stop storage shard
      Await.result(storage.stop(), controlTimeout)
      
      actorSystem.shutdown()
    }
  }
}

// vim: set ts=4 sw=4 et:
