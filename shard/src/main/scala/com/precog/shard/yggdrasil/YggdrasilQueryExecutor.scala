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

import com.precog.common.util._
import com.precog.common.kafka._
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

trait YggdrasilQueryExecutorConfig extends YggEnumOpsConfig with LevelDBQueryConfig with DiskMemoizationConfig with KafkaIngestConfig with BaseConfig with DatasetConsumersConfig{
  lazy val flatMapTimeout: Duration = config[Int]("precog.evaluator.timeout.fm", 30) seconds
  lazy val projectionRetrievalTimeout: Timeout = Timeout(config[Int]("precog.evaluator.timeout.projection", 30) seconds)
  lazy val maxEvalDuration: Duration = config[Int]("precog.evaluator.timeout.eval", 90) seconds
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
      val chunkSerialization = SimpleProjectionSerialization
      val memoizationBufferSize = sortBufferSize
      val memoizationWorkDir = scratchDir
    }
  }
    
  def queryExecutorFactory(queryExecutorConfig: Configuration): QueryExecutor = queryExecutorFactory()
  
  def queryExecutorFactory(): QueryExecutor = {
    val validatedQueryExecutor: IO[Validation[Extractor.Error, QueryExecutor]] = 
      for( yConfig <- loadConfig;
           state   <- YggState.restore(yConfig.dataDir) ) yield {

        state map { yState => new YggdrasilQueryExecutor {
          lazy val actorSystem = ActorSystem("akka_ingest_server")
          implicit lazy val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

          val yggConfig = yConfig
          val centralZookeeperHosts = yConfig.config[String]("precog.kafka.consumer.zk.connect", "localhost:2181") 

          private val coordination = ZookeeperSystemCoordination.testZookeeperSystemCoordination(centralZookeeperHosts)

          object ops extends Ops 
          object query extends QueryAPI 
          object storage extends Storage {
            val yggState = yState
            //val kafkaIngestConfig = yConfig
            val shardId = "shard" + System.getProperty("precog.shard.suffix", "")
            val yggCheckpoints = new SystemCoordinationYggCheckpoints(shardId, coordination) 
            val batchConsumer = new KafkaBatchConsumer("devqclus03.reportgrid.com", 9092, yggConfig.kafkaEventTopic) 
          }
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
  trait Storage extends ActorYggShard with KafkaIngester

  val actorSystem: ActorSystem

  def startup() = storage.start
  def shutdown() = storage.stop map { _ => actorSystem.shutdown } 

  def execute(userUID: String, query: String) = executeWithError(userUID, query).fold(x => x, x => x)

  def executeWithError(userUID: String, query: String) = {
    try {
      asBytecode(query) match {
        case Right(bytecode) => 
          decorate(bytecode) match {
            case Right(dag)  => Right(JString(evaluateDag(userUID, dag)))
            case Left(error) => Left(JString("An error occurred in query analysis: %s".format(error)))
          }
        
        case Left(error) => Left(JString(error))
      }
    } catch {
      // Need to be more specific here or maybe change execute to explicitly return errors 
      case ex: Exception => {
        System.err.println("Error processing query: " + ex)
        ex.printStackTrace
        Left(JString("Error processing query: %s".format(ex.getMessage)))
      }
    }
  }

  private def evaluateDag(userUID: String, dag: DepGraph) = {
    consumeEval(userUID, dag) map { _._2 } map SValue.asJSON mkString ("[", ",", "]")
  }

  private def asBytecode(query: String): Either[String, Vector[Instruction]] = {
    try {
      val tree = compile(query)
      if (tree.errors.isEmpty) Right(emit(tree)) 
      else Left("Errors occurred compiling your query: %s".format(tree.errors.mkString("\n\n")))
    } catch {
      case ex: ParseException => Left("An error occurred parsing your query: " + ex.getMessage)
    }
  }
}

