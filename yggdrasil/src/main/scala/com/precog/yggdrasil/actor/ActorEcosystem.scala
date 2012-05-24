package com.precog.yggdrasil
package actor

import akka.actor._
import akka.dispatch._
import akka.util._
import akka.util.duration._
import akka.pattern.ask
import akka.pattern.gracefulStop

import com.precog.util._
import com.precog.common._
//import com.precog.common.kafka._

import com.weiglewilczek.slf4s.Logging

import java.net.InetAddress

import blueeyes.json.JsonAST._

import MetadataActor._

trait ActorEcosystem {
  val actorSystem: ActorSystem

  val ingestActor: ActorRef
  val ingestSupervisor: ActorRef
  val metadataActor: ActorRef
  val projectionsActor: ActorRef

  def actorsStart: Future[Unit]
  def actorsStop: Future[Unit]

  def status: Future[JArray]
}

trait ActorEcosystemConfig extends BaseConfig {
  def statusTimeout: Long = config[Long]("actors.status.timeout", 30000)
  implicit def stopTimeout: Timeout = config[Long]("actors.stop.timeout", 300) seconds

  def metadataSyncPeriod: Duration = config[Int]("actors.metadata.sync_minutes", 5) minutes
  def batchStoreDelay: Duration    = config[Long]("actors.store.idle_millis", 1000) millis
  def batchShutdownCheckInterval: Duration = config[Int]("actors.store.shutdown_check_seconds", 1) seconds
}

abstract class BaseActorEcosystem[Dataset[_]](restoreCheckpoint: YggCheckpoint) extends ActorEcosystem with ProjectionsActorModule[Dataset] with YggConfigComponent with Logging {
  type YggConfig <: ActorEcosystemConfig

  protected implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
  
  val yggState: YggState

  protected val logPrefix: String

  protected val actorsWithStatus: List[ActorRef]

  val ingestSupervisor = {
    actorSystem.actorOf(Props(new IngestSupervisor(ingestActor, projectionsActor, new SingleColumnProjectionRoutingTable,
                                                   yggConfig.batchStoreDelay, actorSystem.scheduler, yggConfig.batchShutdownCheckInterval)), "router")
  }

  //
  // Public actors
  //
  
  val metadataActor = 
    actorSystem.actorOf(Props(new MetadataActor(State(yggState.metadata, Set(), restoreCheckpoint))), "metadata") 
  
  val projectionsActor = 
    actorSystem.actorOf(Props(newProjectionsActor(yggState.descriptorLocator, yggState.descriptorIO)), "projections")

  def actorsStart = Future[Unit] {
    // TODO: reconsider?
    logger.info("Starting actor ecosystem")
  }

  def status: Future[JArray] = {
    implicit val to = Timeout(yggConfig.statusTimeout)

    for (statusResponses <- Future.sequence { actorsWithStatus map { actor => (actor ? Status).mapTo[JValue] } }) 
    yield JArray(statusResponses)
  }

  protected def actorStop(actor: ActorRef, name: String): Future[Unit] = { 
    for {
      _ <- Future(logger.debug(logPrefix + " Stopping " + name + " actor"))
      b <- gracefulStop(actor, yggConfig.stopTimeout.duration)(actorSystem) 
    } yield {
      logger.debug(logPrefix + " Stop call for " + name + " actor returned " + b)  
    }   
  } recover { 
    case e => logger.error("Error stopping " + name + " actor", e)  
  }   

  def actorsStop: Future[Unit] = {
    import yggConfig.stopTimeout

    for {
      _  <- Future(logger.info(logPrefix + " Stopping"))
      _  <- actorStop(ingestSupervisor, "router")
      _  <- actorsStopInternal
      _  <- Future {
              logger.debug(logPrefix + " Stopping actor system")
              actorSystem.shutdown
              logger.info(logPrefix + " Stopped")
            } recover { 
              case e => logger.error(logPrefix + " Error stopping actor system", e)
            }
    } yield ()
  }

  protected def actorsStopInternal: Future[Unit]
}


