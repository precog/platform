package com.precog
package ingest.service

import blueeyes.json.JsonAST._

import common.Config
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
import akka.util.duration._
import akka.util.Duration
import akka.util.Timeout

import scalaz.{Success, Failure, Validation}
import scalaz.effect.IO

import org.streum.configrity.Configuration

import net.lag.configgy.ConfigMap

trait QueryExecutor {
  def execute(query: String): JValue
  def startup: Future[Unit]
  def shutdown: Future[Unit]
}

trait NullQueryExecutor extends QueryExecutor {
  def actorSystem: ActorSystem
  implicit def executionContext: ExecutionContext

  def execute(query: String) = JString("Query service not avaialble")
  def startup = Future(())
  def shutdown = Future { actorSystem.shutdown }
}

trait YggdrasilQueryExecutor 
    extends QueryExecutor
    with LineErrors
    with Compiler
    with Parser
    with TreeShaker
    with ProvenanceChecker
    with Emitter
    with Evaluator
    with DatasetConsumers
    with OperationsAPI 
    with YggdrasilEnumOpsComponent
    with LevelDBQueryComponent { self =>

  type YggConfig = YggEnumOpsConfig with LevelDBQueryConfig
  
  val yggConfig: YggConfig
  val yggState: YggState

  val actorSystem: ActorSystem
  val asyncContext: ExecutionContext
  val controlTimeout: Duration 

  object storage extends ActorYggShard {
    val yggState = self.yggState
    val yggConfig: YggConfig = self.yggConfig
  }

  object ops extends Ops 

  object query extends QueryAPI 
  
  def startup() = storage.start
  def shutdown() = storage.stop map { _ => actorSystem.shutdown } 

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

  private def evaluateDag(dag: DepGraph) = {
    consumeEval(dag) map { _._2 } map SValue.asJSON mkString ("[", ",", "]")
  }

  private def asBytecode(query: String): Either[Set[Error], Vector[Instruction]] = {
    val tree = compile(query)
    val errors = runPhasesInSequence(tree)
    if(errors.size != 0) Left(errors) else Right(emit(tree)) 
  }
}

trait QueryExecutorConfig extends YggEnumOpsConfig with LevelDBQueryConfig with Config {
  val flatMapTimeout: Duration = config[Int]("precog.evaluator.timeout.fm", 30) seconds
  val projectionRetrievalTimeout: Timeout = Timeout(config[Int]("precog.evaluator.timeout.projection", 30) seconds)
}

trait YggdrasilQueryExecutorComponent {
  import blueeyes.json.xschema.Extractor

  def loadConfig: IO[BaseConfig with YggEnumOpsConfig with LevelDBQueryConfig] = IO { 
    new BaseConfig with QueryExecutorConfig {
      val config = Configuration.parse("")  
    }
  }
    
  def queryExecutorFactory(queryExecutorConfig: ConfigMap): QueryExecutor = queryExecutorFactory()
  
  def queryExecutorFactory(): QueryExecutor = {

    val validatedQueryExecutor: IO[Validation[Extractor.Error, QueryExecutor]] = 
      for( yConfig <- loadConfig;
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
