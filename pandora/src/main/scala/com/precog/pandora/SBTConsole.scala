package com.precog.pandora

import com.codecommit.gll.LineStream

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
  val platform = new Compiler with LineErrors with ProvenanceChecker with Emitter with Evaluator with DatasetConsumers with OperationsAPI with AkkaIngestServer with YggdrasilEnumOpsComponent with LevelDBQueryComponent with DiskMemoizationComponent with DAGPrinter { console =>
    import akka.dispatch.Await
    import akka.util.Duration
    import scalaz._

    import java.io.File

    import scalaz.effect.IO
    
    import org.streum.configrity.Configuration
    import org.streum.configrity.io.BlockFormat

    lazy val controlTimeout = Duration(30, "seconds")

    trait YggConfig extends 
        BaseConfig with 
        YggEnumOpsConfig with 
        LevelDBQueryConfig with 
        DiskMemoizationConfig with 
        DatasetConsumersConfig with
        ProductionActorConfig

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
    
    type Storage = ActorYggShard
    object storage extends ActorYggShard with StandaloneActorEcosystem {
      type YggConfig = console.YggConfig
      val yggConfig = console.yggConfig
      val yggState = shardState
    }
    
    object ops extends Ops 
    
    object query extends QueryAPI 

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
