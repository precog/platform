package com.precog
package pandora

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext

import com.codecommit.gll.LineStream

import com.precog._

import common.kafka._
import common.security._

import daze._
import daze.util._
import daze.memoization._

import pandora._

import quirrel._
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import yggdrasil._
import yggdrasil.actor._
import yggdrasil.serialization._

object SBTConsole {
  
  trait Platform  extends muspelheim.ParseEvalStack 
                  with IterableDatasetOpsComponent
                  with LevelDBQueryComponent 
                  with DiskIterableMemoizationComponent 
                  with MemoryDatasetConsumer
                  with DAGPrinter {

    trait YggConfig extends BaseConfig 
                    with YggEnumOpsConfig 
                    with LevelDBQueryConfig 
                    with DiskMemoizationConfig 
                    with DatasetConsumersConfig 
                    with IterableDatasetOpsConfig 
                    with ProductionActorConfig

    override type Dataset[A] = IterableDataset[A]
    override type Memoable[A] = Iterable[A]
  }

  val platform = new Platform { console =>
    import akka.dispatch.Await
    import akka.util.Duration
    import scalaz._

    import java.io.File

    import scalaz.effect.IO
    
    import org.streum.configrity.Configuration
    import org.streum.configrity.io.BlockFormat

    lazy val actorSystem = ActorSystem("sbt_console_actor_system")
    implicit lazy val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

    lazy val controlTimeout = Duration(30, "seconds")

    object yggConfig extends YggConfig {
      lazy val config = Configuration parse {
        Option(System.getProperty("precog.storage.root")) map { "precog.storage.root = " + _ } getOrElse { "" }
      }

      lazy val sortWorkDir = scratchDir
      lazy val memoizationBufferSize = sortBufferSize
      lazy val memoizationWorkDir = scratchDir

      lazy val flatMapTimeout = controlTimeout
      lazy val projectionRetrievalTimeout = akka.util.Timeout(controlTimeout)
      lazy val maxEvalDuration = controlTimeout
      lazy val clock = blueeyes.util.Clock.System

      object valueSerialization extends SortSerialization[SValue] with SValueRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization
      object eventSerialization extends SortSerialization[SEvent] with SEventRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization
      object groupSerialization extends SortSerialization[(SValue, Identities, SValue)] with GroupRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization
      object memoSerialization extends IncrementalSerialization[(Identities, SValue)] with SEventRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization

      //TODO: Get a producer ID
      lazy val idSource = new IdSource {
        private val source = new java.util.concurrent.atomic.AtomicLong
        def nextId() = source.getAndIncrement
      }
    }

    val Success(shardState) = YggState.restore(yggConfig.dataDir).unsafePerformIO
    
    trait Storage extends ActorYggShard[IterableDataset] with StandaloneActorEcosystem {
      type YggConfig = console.YggConfig
      //protected implicit val projectionManifest = implicitly[Manifest[Projection[IterableDataset]]]
      val yggConfig = console.yggConfig
      val yggState = shardState
      val accessControl = new UnlimitedAccessControl()(asyncContext)
    }
    
    object ops extends Ops 
    object query extends QueryAPI 
    object storage extends Storage

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
      Await.result(storage.actorsStart, controlTimeout)
    }
    
    def shutdown() {
      // stop storage shard
      Await.result(storage.actorsStop, controlTimeout)
      
      actorSystem.shutdown()
    }
  }
}

// vim: set ts=4 sw=4 et:
