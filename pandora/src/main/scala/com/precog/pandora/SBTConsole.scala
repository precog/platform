package com.precog
package pandora

import common.kafka._
import common.security._

import daze._
import daze.util._

import pandora._

import quirrel._
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import yggdrasil._
import yggdrasil.actor._
import yggdrasil.jdbm3._
import yggdrasil.memoization._
import yggdrasil.metadata._
import yggdrasil.serialization._
import yggdrasil.table._

import com.precog.util.FilesystemFileOps

import akka.actor.ActorSystem
import akka.dispatch._
import akka.util.Duration

import com.codecommit.gll.LineStream

object SBTConsole {
  
  trait Platform  extends muspelheim.ParseEvalStack[Future] 
                  with MemoryDatasetConsumer[Future]
                  with BlockStoreColumnarTableModule[Future]
                  with JDBMProjectionModule
                  with SystemActorStorageModule
                  with StandaloneShardSystemActorModule {

    trait YggConfig extends BaseConfig 
                    with DatasetConsumersConfig 
                    with StandaloneShardSystemConfig
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

      object valueSerialization extends SortSerialization[SValue] with SValueRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization
      object eventSerialization extends SortSerialization[SEvent] with SEventRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization
      object groupSerialization extends SortSerialization[(SValue, Identities, SValue)] with GroupRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization
      object memoSerialization extends IncrementalSerialization[(Identities, SValue)] with SEventRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization

      //TODO: Get a producer ID
      val idSource = new IdSource {
        private val source = new java.util.concurrent.atomic.AtomicLong
        def nextId() = source.getAndIncrement
      }
    }

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

    def eval(str: String): Set[SValue] = evalE(str)  match {
      case Success(results) => results.map(_._2)
      case Failure(t) => throw t
    }

    def evalE(str: String) = {
      val tree = compile(str)
      if (!tree.errors.isEmpty) {
        sys.error(tree.errors map showError mkString ("Set(\"", "\", \"", "\")"))
      }
      val Right(dag) = decorate(emit(tree))
      withContext { ctx => consumeEval("0", dag, ctx) }
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
