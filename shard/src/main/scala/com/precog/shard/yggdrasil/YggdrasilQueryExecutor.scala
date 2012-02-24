package com.precog
package shard
package yggdrasil 

import blueeyes.json.JsonAST._

import common._
import daze._

import quirrel.Compiler
import quirrel.LineErrors
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import com.precog.yggdrasil._
import com.precog.yggdrasil.shard._

import akka.actor.ActorSystem
import akka.dispatch._
import akka.util.duration._
import akka.util.Duration
import akka.util.Timeout

import scalaz.{Success, Failure, Validation}
import scalaz.effect.IO

import org.streum.configrity.Configuration

trait YggdrasilQueryExecutorConfig extends YggEnumOpsConfig with LevelDBQueryConfig with DiskMemoizationConfig with KafkaIngestConfig with BaseConfig {
  lazy val flatMapTimeout: Duration = config[Int]("precog.evaluator.timeout.fm", 30) seconds
  lazy val projectionRetrievalTimeout: Timeout = Timeout(config[Int]("precog.evaluator.timeout.projection", 30) seconds)
}

trait YggdrasilQueryExecutorComponent {
  import blueeyes.json.xschema.Extractor

  def loadConfig: IO[YggdrasilQueryExecutorConfig] = IO { 
    new YggdrasilQueryExecutorConfig {
      val config = Configuration.parse("""
        precog {
          kafka {
            enabled = true 
            topic {
              events = central_event_store
            }
            consumer {
              zk {
                connect = devqclus03.reportgrid.com:2181 
                connectiontimeout {
                  ms = 1000000
                }
              }
              groupid = shard_consumer
            }
          }
        }
      """)  
      val sortWorkDir = scratchDir
      val sortSerialization = SimpleProjectionSerialization
      val memoizationBufferSize = sortBufferSize
      val memoizationWorkDir = scratchDir
      val memoizationSerialization = SimpleProjectionSerialization
    }
  }
    
  def queryExecutorFactory(queryExecutorConfig: Configuration): QueryExecutor = queryExecutorFactory()
  
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
    with LevelDBQueryComponent 
    with DiskMemoizationComponent { self =>

  type YggConfig = YggdrasilQueryExecutorConfig
  
  val yggState: YggState
  val actorSystem: ActorSystem

  object storage extends ActorYggShard with KafkaIngester with YggConfigComponent {
    type YggConfig = self.YggConfig 
    val yggState = self.yggState
    val yggConfig = self.yggConfig
    val kafkaIngestConfig = self.yggConfig
  }

  object ops extends Ops 

  object query extends QueryAPI 

  def startup() = storage.start flatMap { _ => storage.startKafka }
  def shutdown() = storage.stopKafka flatMap { _ => storage.stop } map { _ => actorSystem.shutdown } 

  def execute(userUID: String, query: String) = {
    try {
      asBytecode(query) match {
        case Right(bytecode) => 
          decorate(bytecode) match {
            case Right(dag)  => JString(evaluateDag(userUID, dag))
            case Left(error) => JString("Error processing dag: %s".format(error.toString))
          }
        
        case Left(errors) => JString("Parsing errors: %s".format(errors.toString))
      }
    } catch {
      // Need to be more specific here or maybe change execute to explicitly return errors 
      case ex: Exception => JString("Error processing query: %s".format(ex.getMessage))
    }
  }

  private def evaluateDag(userUID: String, dag: DepGraph) = {
    consumeEval(userUID, dag) map { _._2 } map SValue.asJSON mkString ("[", ",", "]")
  }

  private def asBytecode(query: String): Either[Set[Error], Vector[Instruction]] = {
    val tree = compile(query)
    if(tree.errors.isEmpty) Right(emit(tree)) else Left(tree.errors)
  }
}

