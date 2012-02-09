package com.precog
package ingest.service

import blueeyes.json.JsonAST._

import daze._

import quirrel.Compiler
import quirrel.LineErrors
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import yggdrasil._
import yggdrasil.shard._

import akka.actor.ActorSystem
import akka.dispatch._
import akka.util.Duration

import scalaz.{Success, Failure, Validation}
import scalaz.effect.IO

import org.streum.configrity.Configuration

import net.lag.configgy.ConfigMap

trait QueryExecutor {
  def execute(query: String): JValue
  def startup(): Future[Unit]
  def shutdown(): Future[Unit]
}

trait QueryExecutorComponent {
  def queryExecutorFactory(configMap: ConfigMap): QueryExecutor
}

trait NullQueryExecutor extends QueryExecutor {

  val actorSystem: ActorSystem
  lazy implicit val executionContext: ExecutionContext = ExecutionContext.defaultExecutionContext(actorSystem)

  def execute(query: String) = JString("Query service not avaialble")
  def startup() = Future(())
  def shutdown() = Future { actorSystem.shutdown }

}

trait NullQueryExecutorComponent {
  def queryExecutorFactory(configMap: ConfigMap) = new NullQueryExecutor {
    lazy val actorSystem = ActorSystem("null_query_executor")
  }
}

trait QuirrelQueryExecutor extends QueryExecutor 
    with LineErrors
    with Compiler
    with Parser
    with TreeShaker
    with ProvenanceChecker
    with Emitter
    with Evaluator
    with DatasetConsumers
    with OperationsAPI { 

  def execute(query: String) = {
    asBytecode(query) match {
      case Right(bytecode) => 
        decorate(bytecode) match {
          case Right(dag)  => JString(evaluateDag(dag))
          case Left(error) => JString("Error processing dag: %s".format(error.toString))
        }
      
      case Left(errors) => JString("Parsing errors: %s".format(errors.toString))
    }
  }

  def evaluateDag(dag: DepGraph) = {
    consumeEval(dag) map { _._2 } map SValue.asJSON mkString ("[", ",", "]")
  }

  def asBytecode(query: String): Either[Set[Error], Vector[Instruction]] = {
    val tree = compile(query)
    val errors = runPhasesInSequence(tree)
    if(errors.size != 0) Left(errors) else Right(emit(tree)) 
  }
}

trait YggdrasilQueryExecutor extends QuirrelQueryExecutor { self =>

    val yggConfig: YggConfig
    val yggState: YggState

    val actorSystem: ActorSystem
    val asyncContext: ExecutionContext
    val controlTimeout: Duration 

    object storage extends ActorYggShard {
      val yggState = self.yggState
      val yggConfig: YggConfig = self.yggConfig
    }

    object ops extends YggdrasilEnumOps {
      val yggConfig: YggConfig = self.yggConfig 
      val asyncContext = self.asyncContext
      val flatMapTimeout = akka.util.Timeout(controlTimeout)
    }
 
    object query extends LevelDBQueryAPI {
      val storage = self.storage
      val asyncContext = self.asyncContext
      val projectionRetrievalTimeout = akka.util.Timeout(controlTimeout)
    }

    def startup() = storage.start
    def shutdown() = storage.stop map { _ => actorSystem.shutdown } 

}

trait YggdrasilQueryExecutorComponent {

  import blueeyes.json.xschema.Extractor

  def loadConfig() = IO { new YggConfig { def config = Configuration.parse("") } }
    
  def queryExecutorFactory(queryExecutorConfig: ConfigMap): QueryExecutor = queryExecutorFactory()
  
  def queryExecutorFactory(): QueryExecutor = {

    val validatedQueryExecutor: IO[Validation[Extractor.Error, QueryExecutor]] = 
      for( yConfig <- loadConfig();
           state   <- YggState.restore(yConfig.dataDir) ) yield {

        state map { yState => new YggdrasilQueryExecutor {
          val controlTimeout = Duration(120, "seconds")
          val maxEvalDuration = controlTimeout 

          lazy val actorSystem = ActorSystem("akka_ingest_server")
          implicit lazy val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

          val yggConfig = yConfig
          val yggState = yState


        }}
      }

    validatedQueryExecutor map { 
      case Success(qs) => qs
      case Failure(er) => sys.error("Error initializing query service: " + er)
    } unsafePerformIO

  }

}

object QuickQueryExecutor extends App with YggdrasilQueryExecutorComponent {

  val qs = queryExecutorFactory()

  qs.execute(args(0)) match {
    case JString(s) => println(s)
    case _          => println("Unexpected result.")
  }
}
