package com.precog.pandora

import edu.uwm.cs.gll.LineStream

import com.precog._
import com.precog.yggdrasil.shard._
import com.precog.common.kafka._

import daze._
import daze.util._

import pandora._

import quirrel._
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import yggdrasil._
import yggdrasil.shard._

object SBTConsole {
  val platform = new Compiler with LineErrors with ProvenanceChecker with Emitter with Evaluator with DatasetConsumers with OperationsAPI with AkkaIngestServer with YggdrasilEnumOpsComponent with LevelDBQueryComponent with DiskMemoizationComponent with DAGPrinter {
    import akka.dispatch.Await
    import akka.util.Duration
    import scalaz._

    import java.io.File

    import scalaz.effect.IO
    
    import org.streum.configrity.Configuration
    import org.streum.configrity.io.BlockFormat

    lazy val controlTimeout = Duration(30, "seconds")

    trait YggConfig extends BaseConfig with YggEnumOpsConfig with LevelDBQueryConfig with DiskMemoizationConfig with DatasetConsumersConfig
    object yggConfig extends YggConfig {
      lazy val config = Configuration parse {
        Option(System.getProperty("precog.storage.root")) map { "precog.storage.root = " + _ } getOrElse { "" }
      }

      lazy val flatMapTimeout = controlTimeout
      lazy val projectionRetrievalTimeout = akka.util.Timeout(controlTimeout)
      lazy val chunkSerialization = SimpleProjectionSerialization
      lazy val sortWorkDir = scratchDir
      lazy val memoizationBufferSize = sortBufferSize
      lazy val memoizationWorkDir = scratchDir
      lazy val maxEvalDuration = controlTimeout
    }

    val Success(shardState) = YggState.restore(yggConfig.dataDir).unsafePerformIO
    
    object storage extends ActorYggShard {
      val yggState = shardState
      val yggCheckpoints = new TestYggCheckpoints
      val batchConsumer = BatchConsumer.NullBatchConsumer
    }
    
    object ops extends Ops 
    
    object query extends QueryAPI 

    def eval(str: String): Set[SValue] = evalE(str) map { _._2 }

    def evalE(str: String) = {
      val tree = compile(str)
      if (!tree.errors.isEmpty) {
        sys.error(tree.errors map showError mkString ("Set(\"", "\", \"", "\")"))
      }
      val Right(dag) = decorate(emit(tree))
      consumeEval("0", dag)
    }

    def startup() {
      // start storage shard 
      Await.result(storage.start, controlTimeout)
    }
    
    def shutdown() {
      // stop storage shard
      Await.result(storage.stop, controlTimeout)
      
      actorSystem.shutdown()
    }
  }
}

// vim: set ts=4 sw=4 et:
