package com.precog.yggdrasil
package actor

import akka.actor._
import akka.dispatch._
import akka.util._
import akka.util.duration._
import akka.pattern.ask
import akka.pattern.gracefulStop

import com.precog.common.util._
import com.precog.common.kafka._

import com.weiglewilczek.slf4s.Logging

import java.net.InetAddress

import blueeyes.json.JsonAST._

trait ActorEcosystem {
  val actorSystem: ActorSystem
  val metadataActor: ActorRef
  val projectionActors: ActorRef
  val routingActor: ActorRef
  def actorsStart: Future[Unit]
  def actorsStop: Future[Unit]
  def actorsStatus: Future[JArray]
}

trait ActorEcosystemConfig extends BaseConfig {
  def shardId: String = serviceUID.hostId + serviceUID.serviceId 

  def statusTimeout: Long = config[Long]("actors.status.timeout", 30000)
  implicit def stopTimeout: Timeout = config[Long]("actors.stop.timeout", 300) seconds

  def serviceUID: ServiceUID = ZookeeperSystemCoordination.extractServiceUID(config)

  def metadataSyncPeriod: Duration = config[Int]("actors.metadata.sync_minutes", 5) minutes
  def batchStoreDelay: Duration    = config[Long]("actors.store.idle_millis", 1000) millis
  def batchShutdownCheckInterval: Duration = config[Int]("actors.store.shutdown_check_seconds", 1) seconds
}

// A case object used as a request for status information across actor types
case object Status

trait BaseActorEcosystem extends ActorEcosystem with YggConfigComponent with Logging {
  type YggConfig <: ActorEcosystemConfig
  
  def yggState(): YggState

  protected val pre: String
  protected lazy implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)

  protected lazy val routingDispatch = new RoutingDispatch(new SingleColumnProjectionRoutingTable, projectionActors, metadataActor, Duration(60, "seconds"))(new Timeout(60000), ExecutionContext.defaultExecutionContext(actorSystem))

  lazy val metadataActor = {
    val localMetadata = new LocalMetadata(yggState.metadata, checkpoints.latestCheckpoint.messageClock)
    actorSystem.actorOf(Props(new MetadataActor(localMetadata)), "metadata") 
  }
  
  lazy val projectionActors = {
    actorSystem.actorOf(Props(new ProjectionActors(yggState.descriptorLocator, yggState.descriptorIO, actorSystem.scheduler)), "projections")
  }

  protected lazy val metadataSerializationActor = {
    val metadataStorage = new FilesystemMetadataStorage(yggState.descriptorLocator)
    actorSystem.actorOf(Props(new MetadataSerializationActor(checkpoints, metadataStorage)), "metadata_serializer")
  }
  
  protected lazy val metadataSyncCancel = {
    actorSystem.scheduler.schedule(yggConfig.metadataSyncPeriod, yggConfig.metadataSyncPeriod, metadataActor, FlushMetadata(metadataSerializationActor))
  }

  protected val checkpoints: YggCheckpoints 

  def actorsStart = Future[Unit] {
    logger.info("Starting actor ecosystem")
    this.metadataSyncCancel
    routingActor ! Start 
  }

  protected def actorStop(actor: ActorRef, name: String): Future[Unit] = { 
    for {
      _ <- Future(logger.debug(pre + "Stopping " + name + " actor"))
      b <- gracefulStop(actor, yggConfig.stopTimeout.duration)(actorSystem) 
    } yield {
      logger.debug(pre + "Stop call for " + name + " actor returned " + b)  
    }   
  } recover { 
    case e => logger.error("Error stopping " + name + " actor", e)  
  }   

  def actorsStop: Future[Unit] = {
    import logger._
    import yggConfig.stopTimeout

    def routingActorStop = for {
      _ <- Future(logger.debug(pre + "Sending controlled stop message to routing actor"))
      _ <- (routingActor ? ControlledStop) recover { case e => error("Controlled stop failed for routing actor.", e) }
      _ <- actorStop(routingActor, "routing")
    } yield () 

    def flushMetadata = {
      logger.debug(pre + "Flushing metadata")
      (metadataActor ? FlushMetadata(metadataSerializationActor)) recover { case e => error("Error flushing metadata", e) }
    }

    for {
      _  <- Future(info(pre + "Stopping"))
      _  <- Future {
              logger.debug(pre + "Stopping metadata sync")
              metadataSyncCancel.cancel
            }
      _  <- routingActorStop
      _  <- flushMetadata
      _  <- actorsStopInternal
      _  <- Future {
              logger.debug(pre + "Stopping actor system")
              actorSystem.shutdown
              info(pre + "Stopped")
            } recover { 
              case e => error("Error stopping actor system", e)
            }
    } yield ()
  }

  protected def actorsStopInternal: Future[Unit]
}


