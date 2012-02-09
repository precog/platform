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

import scalaz.{Success, Failure}
import scalaz.effect.IO

import org.streum.configrity.Configuration

import net.lag.configgy.ConfigMap

trait QueryService {
  def execute(query: String): JValue
  def startup(): Future[Unit]
  def shutdown(): Future[Unit]
}

trait NullQueryService extends QueryService {

  val actorSystem: ActorSystem
  implicit val executionContext: ExecutionContext = ExecutionContext.defaultExecutionContext(actorSystem)

  def execute(query: String) = JString("Query service not avaialble")
  def startup() = Future(())
  def shutdown() = Future { actorSystem.shutdown }
}

trait QuirrelQueryService extends QueryService 
    with LineErrors
    with Compiler
    with Parser
    with TreeShaker
    with ProvenanceChecker
    with Emitter
    with Evaluator
    with DatasetConsumers
    with YggdrasilOperationsAPI 
    with YggdrasilStorage {

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

trait HardwiredQueryService extends QuirrelQueryService {
    lazy val actorSystem = ActorSystem("akka_ingest_server")
    implicit lazy val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

    val yggConfig = IO { new YggConfig { def config = Configuration.parse("") } }
    
    private val yggShard: IO[YggShard] = yggConfig flatMap { cfg => YggState.restore(cfg.dataDir) map { (cfg, _) } } map { 
      case (cfg, Success(state)) =>
        new RealYggShard {
         val yggState = state
          val yggConfig = cfg
        }
      case (cfg, Failure(e)) => sys.error("Error loading shard state from: %s cause:\n".format(cfg.dataDir, e))
    }
  
    lazy val storage: YggShard = yggShard.unsafePerformIO

    def startup() = storage.start
    def shutdown() = storage.stop map { _ => actorSystem.shutdown } 

}

trait HardwiredQueryServiceFactory {
  def queryServiceFactory(queryServiceConfig: ConfigMap): QueryService = {
    new HardwiredQueryService { }
  }
}

object QuickQueryService extends App {
  object qs extends HardwiredQueryService

  qs.execute(args(0)) match {
    case JString(s) => println(s)
    case _          => println("Unexpected result.")
  }
}
