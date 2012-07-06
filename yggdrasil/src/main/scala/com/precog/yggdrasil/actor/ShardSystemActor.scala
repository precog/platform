/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.yggdrasil
package actor

import com.precog.common.{CheckpointCoordination,YggCheckpoint}
import com.precog.util.FilesystemFileOps
import com.precog.yggdrasil.metadata.{FileMetadataStorage,MetadataStorage}

import akka.actor._
import akka.dispatch._
import akka.util._
import akka.util.duration._
import akka.pattern.ask
import akka.pattern.gracefulStop

import blueeyes.json.JsonAST._

import com.weiglewilczek.slf4s.Logging

import scalaz.{Failure,Success}
import scalaz.syntax.std.boolean._

case object ShutdownSystem
case object ShutdownComplete

trait ShardConfig extends BaseConfig {
  def shardId: String
  def logPrefix: String

  def ingestEnabled: Boolean = config[Boolean]("ingest_enabled", true)

  def statusTimeout: Long = config[Long]("actors.status.timeout", 30000)
  def metadataTimeout: Timeout = config[Long]("actors.metadata.timeout", 30) seconds
  implicit def stopTimeout: Timeout = config[Long]("actors.stop.timeout", 300) seconds

  def metadataSyncPeriod: Duration = config[Int]("actors.metadata.sync_minutes", 1) minutes
  def batchStoreDelay: Duration    = config[Long]("actors.store.idle_millis", 1000) millis
  def batchShutdownCheckInterval: Duration = config[Int]("actors.store.shutdown_check_seconds", 1) seconds

  def initIngestActor(checkpoint: YggCheckpoint, metadataActor: ActorRef): Option[Actor]

  def checkpointCoordination: CheckpointCoordination
}

abstract class ShardSystemActor[Config <: ShardConfig, Dataset[_]](config: Config, storage: MetadataStorage) extends Actor with ProjectionsActorModule[Dataset] with Logging {
  // The ingest system consists of the ingest supervisor and ingest actor(s)
  private[this] var ingestSystem: Option[ActorRef]    = None
  private[this] var metadataActor: ActorRef           = _
  private[this] var projectionsActor: ActorRef        = _
  private[this] var metadataSync: Option[Cancellable] = None

  private def loadCheckpoint() : Option[YggCheckpoint] = config.ingestEnabled.option(YggCheckpoint.Empty) flatMap { 
    _ =>
    config.checkpointCoordination.loadYggCheckpoint(config.shardId) match {
      case Some(Failure(errors)) =>
        logger.error("Unable to load Kafka checkpoint: " + errors)
        sys.error("Unable to load Kafka checkpoint: " + errors)

      case Some(Success(checkpoint)) => Some(checkpoint)
      case None => None
    }
  }

  override def preStart() {
    val initialCheckpoint = loadCheckpoint()

    logger.info("Initializing MetadataActor with storage = " + storage)
    metadataActor    = context.actorOf(Props(new MetadataActor(config.shardId, storage, config.checkpointCoordination, initialCheckpoint)), "metadata")

    logger.debug("Initializing ProjectionsActor")
    projectionsActor = context.actorOf(Props(newProjectionsActor(metadataActor, config.metadataTimeout)), "projections")

    ingestSystem     = initialCheckpoint map {
      checkpoint =>

      logger.debug("Initializing ingest system")
      // Ingest implies a metadata sync
      metadataSync = Some(context.system.scheduler.schedule(config.metadataSyncPeriod, config.metadataSyncPeriod, metadataActor, FlushMetadata))

      context.actorOf(Props(new IngestSupervisor(() => config.initIngestActor(checkpoint, metadataActor), projectionsActor, new SingleColumnProjectionRoutingTable,
                                                 config.batchStoreDelay, context.system.scheduler, config.batchShutdownCheckInterval)), "ingestRouter")
    }
  }

  def receive = {
    // Route subordinate messages
    case pMsg: ShardProjectionAction => projectionsActor.tell(pMsg, sender)
    case mMsg: ShardMetadataAction   => metadataActor.tell(mMsg, sender)
    case iMsg: ShardIngestAction     => ingestSystem.foreach(_.tell(iMsg, sender))

    case Status => {
      implicit val to = Timeout(config.statusTimeout)
      implicit val execContext = ExecutionContext.defaultExecutionContext(context.system)
      
      sender ! (for (statusResponses <- Future.sequence { actorsWithStatus map { actor => (actor ? Status).mapTo[JValue] } }) yield JArray(statusResponses))
    }

    case ShutdownSystem => {
      onShutdown()
      sender ! ShutdownComplete
      self ! PoisonPill
    }
  }

  protected def actorsWithStatus = ingestSystem.toList ++
                                   (metadataActor :: projectionsActor :: Nil)

  protected def onShutdown(): Future[Unit] = {
    implicit val execContext = ExecutionContext.defaultExecutionContext(context.system)
    for {
      _ <- Future(logger.info("Stopping shard system"))
      _ <- metadataSync.map(timer => Future{ timer.cancel() }).getOrElse(Future(()))
      _ <- ingestSystem.map{ actor => actorStop(actor, "ingest") }.getOrElse(Future(()))
      _ <- actorStop(projectionsActor, "projections")
      _ <- actorStop(metadataActor, "metadata")
    } yield ()
  }

  protected def actorStop(actor: ActorRef, name: String)(implicit executor: ExecutionContext): Future[Unit] = { 
    for {
      _ <- Future(logger.debug(config.logPrefix + " Stopping " + name + " actor within " + config.stopTimeout.duration))
      b <- gracefulStop(actor, config.stopTimeout.duration)(context.system) 
    } yield {
      logger.debug(config.logPrefix + " Stop call for " + name + " actor returned " + b)  
    }   
  } recover { 
    case e => logger.error("Error stopping " + name + " actor", e)  
  }   

}
