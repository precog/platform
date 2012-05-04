package com.precog
package yggdrasil
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

import blueeyes.json.JsonAST._

case object Status

trait ActorEcosystem {
  def actorSystem(): ActorSystem
  def metadataActor(): ActorRef
  def projectionActors(): ActorRef
  def routingActor(): ActorRef
  def actorsStart(): Future[Unit]
  def actorsStop(): Future[Unit]
  def actorsStatus(): Future[JArray]
}

trait ProductionActorConfig extends BaseConfig {
  def shardId(): String = "shard" + System.getProperty("precog.shard.suffix", "") 

  def kafkaHost(): String = config[String]("kafka.batch.host")
  def kafkaPort(): Int = config[Int]("kafka.batch.port")
  def kafkaTopic(): String = config[String]("kafka.batch.topic") 

  def zookeeperHosts(): String = config[String]("zookeeper.hosts")
  def zookeeperBase(): List[String] = config[List[String]]("zookeeper.basepath")
  def zookeeperPrefix(): String = config[String]("zookeeper.prefix")   

  def statusTimeout(): Long = config[Long]("actors.status.timeout", 30000)
}

trait ProductionActorEcosystem extends ActorEcosystem with Logging {

  val pre = "[Yggdrasil Shard]"

  type YggConfig <: ProductionActorConfig

  def yggState(): YggState
  def yggConfig(): YggConfig

  lazy val actorSystem = ActorSystem("production_actor_system")
  private lazy implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)

  lazy val metadataActor = {
    val localMetadata = new LocalMetadata(yggState.metadata, checkpoints.latestCheckpoint.messageClock)
    actorSystem.actorOf(Props(new MetadataActor(localMetadata)), "metadata") 
  }
  
  lazy val projectionActors = {
    actorSystem.actorOf(Props(new ProjectionActors(yggState.descriptorLocator, yggState.descriptorIO, actorSystem.scheduler)), "projections")
  }
  
  lazy val routingActor = {
    val routingTable = new SingleColumnProjectionRoutingTable
    val eventStore = new EventStore(routingTable, projectionActors, metadataActor, Duration(60, "seconds"), new Timeout(60000), ExecutionContext.defaultExecutionContext(actorSystem))
    actorSystem.actorOf(Props(new BatchStoreActor(eventStore, 1000, Some(ingestActor), actorSystem.scheduler)), "router")
  }
  
  def actorsStart() = Future[Unit] {
    this.metadataSyncCancel
    routingActor ! Start 
  }

  private lazy val actorsWithStatus = List(
    projectionActors,
    metadataActor,
    routingActor,
    ingestActor,
    metadataSerializationActor
  )

  def actorsStatus(): Future[JArray] = {
    implicit val to = Timeout(yggConfig.statusTimeout)
    Future.sequence( actorsWithStatus.map { actor => (actor ? Status).mapTo[JValue] } ).map { JArray(_) }
  }

  def actorsStop(): Future[Unit] = {
    import logger._

    val defaultSystem = actorSystem
    val defaultTimeout = 300 seconds
    implicit val timeout: Timeout = defaultTimeout

    def actorStop(actor: ActorRef, name: String): Future[Unit] = { 
      for {
        _ <- Future(debug(pre + "Stopping " + name + " actor"))
        b <- gracefulStop(actor, defaultTimeout)(defaultSystem) 
      } yield {
        debug(pre + "Stop call for " + name + " actor returned " + b)  
      }   
    } recover { 
      case e => error("Error stopping " + name + " actor", e)  
    }   

    def routingActorStop = for {
      _ <- Future(debug(pre + "Sending controlled stop message to routing actor"))
      _ <- (routingActor ? ControlledStop) recover { case e => error("Controlled stop failed for routing actor.", e) }
      _ <- actorStop(routingActor, "routing")
    } yield () 

    def flushMetadata = {
      debug(pre + "Flushing metadata")
      (metadataActor ? FlushMetadata(metadataSerializationActor)) recover { case e => error("Error flushing metadata", e) }
    }

    for {
      _  <- Future(info(pre + "Stopping"))
      _  <- Future {
              debug(pre + "Stopping metadata sync")
              metadataSyncCancel.cancel
            }
      _  <- routingActorStop
      _  <- flushMetadata
      _  <- actorStop(ingestActor, "ingest")
      _  <- actorStop(projectionActors, "projection")
      _  <- actorStop(metadataActor, "metadata")
      _  <- actorStop(metadataSerializationActor, "flush")
      _  <- Future {
              debug(pre + "Stopping actor system")
              actorSystem.shutdown
              info(pre + "Stopped")
            } recover { 
              case e => error("Error stopping actor system", e)
            }
    } yield ()
  }

  //
  // Internal only actors
  //
  
  private val metadataSyncPeriod = Duration(1, "minutes")
  
  
  private lazy val ingestBatchConsumer = {
    new KafkaBatchConsumer(yggConfig.kafkaHost, yggConfig.kafkaPort, yggConfig.kafkaTopic)
  }

  private lazy val ingestActor = {
    actorSystem.actorOf(Props(new KafkaShardIngestActor(checkpoints, ingestBatchConsumer)), "shard_ingest")
  }
 
  private lazy val metadataStorage = {
    new FilesystemMetadataStorage(yggState.descriptorLocator)
  }

  private lazy val metadataSerializationActor = {
    actorSystem.actorOf(Props(new MetadataSerializationActor(checkpoints, metadataStorage)), "metadata_serializer")
  }

  
  private lazy val metadataSyncCancel = actorSystem.scheduler.schedule(metadataSyncPeriod, metadataSyncPeriod, metadataActor, FlushMetadata(metadataSerializationActor))
  
  private lazy val systemCoordination = {
    ZookeeperSystemCoordination(yggConfig.zookeeperHosts, yggConfig.zookeeperBase, yggConfig.zookeeperPrefix) 
  }
  
  private lazy val checkpoints = new SystemCoordinationYggCheckpoints(yggConfig.shardId, systemCoordination)
}

trait StandaloneActorEcosystem extends ActorEcosystem with Logging {
  type YggConfig <: ProductionActorConfig
  
  val pre = "[Yggdrasil Shard]"

  def yggState(): YggState
  def yggConfig(): YggConfig

  lazy val actorSystem = ActorSystem("standalone_actor_system")
  private lazy implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)

  lazy val metadataActor = {
    val localMetadata = new LocalMetadata(yggState.metadata, checkpoints.latestCheckpoint.messageClock)
    actorSystem.actorOf(Props(new MetadataActor(localMetadata)), "metadata") 
  }
  
  lazy val projectionActors = {
    actorSystem.actorOf(Props(new ProjectionActors(yggState.descriptorLocator, yggState.descriptorIO, actorSystem.scheduler)), "projections")
  }
  
  lazy val routingActor = {
    val routingTable = new SingleColumnProjectionRoutingTable
    val eventStore = new EventStore(routingTable, projectionActors, metadataActor, Duration(60, "seconds"), new Timeout(60000), ExecutionContext.defaultExecutionContext(actorSystem))
    actorSystem.actorOf(Props(new BatchStoreActor(eventStore, 1000, None, actorSystem.scheduler)), "router")
  }
  
  def actorsStart() = Future[Unit] {
    this.metadataSyncCancel
    routingActor ! Start
  }
  
  def actorsStatus(): Future[JArray] = Future {
    JArray(List(JString("StandaloneActorEcosystem status not yet implemented.")))
  }

  def actorsStop(): Future[Unit] = {
    import logger._

    val defaultSystem = actorSystem
    val defaultTimeout = 300 seconds
    implicit val timeout: Timeout = defaultTimeout

    def actorStop(actor: ActorRef, name: String): Future[Unit] = { 
      for {
        _ <- Future(debug(pre + "Stopping " + name + " actor"))
        b <- gracefulStop(actor, defaultTimeout)(defaultSystem) 
      } yield {
        debug(pre + "Stop call for " + name + " actor returned " + b)  
      }   
    } recover { 
      case e => error("Error stopping " + name + " actor", e)  
    }   

    def routingActorStop = for {
      _ <- Future(debug(pre + "Sending controlled stop message to routing actor"))
      _ <- (routingActor ? ControlledStop) recover { case e => error("Controlled stop failed for routing actor.", e) }
      _ <- actorStop(routingActor, "routing")
    } yield () 

    def flushMetadata = {
      debug(pre + "Flushing metadata")
      (metadataActor ? FlushMetadata(metadataSerializationActor)) recover { case e => error("Error flushing metadata", e) }
    }

    for {
      _  <- Future(info(pre + "Stopping"))
      _  <- Future {
              debug(pre + "Stopping metadata sync")
              metadataSyncCancel.cancel
            }
      _  <- routingActorStop
      _  <- flushMetadata
      _  <- actorStop(projectionActors, "projection")
      _  <- actorStop(metadataActor, "metadata")
      _  <- actorStop(metadataSerializationActor, "flush")
      _  <- Future {
              debug(pre + "Stopping actor system")
              actorSystem.shutdown
              info(pre + "Stopped")
            } recover { 
              case e => error("Error stopping actor system", e)
            }
    } yield ()
  }
  
  private val metadataSyncPeriod = Duration(1, "minutes")
  
  private lazy val metadataStorage = {
    new FilesystemMetadataStorage(yggState.descriptorLocator)
  }
  
  private lazy val metadataSerializationActor = {
    actorSystem.actorOf(Props(new MetadataSerializationActor(checkpoints, metadataStorage)), "metadata_serializer")
  }

  
  private lazy val metadataSyncCancel = actorSystem.scheduler.schedule(metadataSyncPeriod, metadataSyncPeriod, metadataActor, FlushMetadata(metadataSerializationActor))
  
  private lazy val checkpoints = new YggCheckpoints {
    def saveRecoveryPoint(checkpoints: YggCheckpoint) { }
  }
}
