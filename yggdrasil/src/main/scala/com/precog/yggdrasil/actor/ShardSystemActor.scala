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

import com.precog.common.{ Archive, ArchiveMessage, CheckpointCoordination, IngestMessage, YggCheckpoint }
import com.precog.common.json._
import com.precog.util.FilesystemFileOps
import com.precog.yggdrasil.metadata.{ ColumnMetadata, FileMetadataStorage, MetadataStorage }

import akka.actor._
import akka.dispatch._
import akka.util._
import akka.util.duration._
import akka.pattern.ask
import akka.pattern.gracefulStop

import blueeyes.json.JPath
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
  def stopTimeout: Timeout = config[Long]("actors.stop.timeout", 300) seconds

  def maxOpenProjections: Int = config[Int]("actors.store.max_open_projections", 5000)

  def metadataSyncPeriod: Duration = config[Int]("actors.metadata.sync_minutes", 1) minutes
  def batchStoreDelay: Duration    = config[Long]("actors.store.idle_millis", 1000) millis
  def batchShutdownCheckInterval: Duration = config[Int]("actors.store.shutdown_check_seconds", 1) seconds
}

trait ShardSystemActorModule extends ProjectionsActorModule with YggConfigComponent {
  type YggConfig <: ShardConfig

  protected def initIngestActor(checkpoint: YggCheckpoint, metadataActor: ActorRef): Option[() => Actor]

  protected def checkpointCoordination: CheckpointCoordination

  class ShardSystemActor(storage: MetadataStorage) extends Actor with Logging {

    // The ingest system consists of the ingest supervisor and ingest actor(s)
    private[this] var ingestSystem: ActorRef            = _
    private[this] var metadataActor: ActorRef           = _
    private[this] var projectionsActor: ActorRef        = _
    private[this] var metadataSync: Option[Cancellable] = None

    private def loadCheckpoint() : Option[YggCheckpoint] = 
      if (yggConfig.ingestEnabled) {
        checkpointCoordination.loadYggCheckpoint(yggConfig.shardId) match {
          case Some(Failure(errors)) =>
            logger.error("Unable to load Kafka checkpoint: " + errors)
            sys.error("Unable to load Kafka checkpoint: " + errors)

          case Some(Success(checkpoint)) => Some(checkpoint)
          case None => None
        }
      } else {
        None
      }

    override def preStart() {
      val initialCheckpoint = loadCheckpoint()

      logger.info("Initializing MetadataActor with storage = " + storage)
      metadataActor = context.actorOf(Props(new MetadataActor(yggConfig.shardId, storage, checkpointCoordination, initialCheckpoint)), "metadata")

      logger.debug("Initializing ProjectionsActor")
      projectionsActor = context.actorOf(Props(new ProjectionsActor(yggConfig.maxOpenProjections)), "projections")

      val ingestActorInit: Option[() => Actor] = initialCheckpoint flatMap {
        checkpoint: YggCheckpoint => initIngestActor(checkpoint, metadataActor)
      }
 
      ingestSystem     = { 
        logger.debug("Initializing ingest system")
        // Ingest implies a metadata sync
        metadataSync = Some(context.system.scheduler.schedule(yggConfig.metadataSyncPeriod, yggConfig.metadataSyncPeriod, metadataActor, FlushMetadata))

        val routingTable = new SingleColumnProjectionRoutingTable

        context.actorOf(Props(new IngestSupervisor(ingestActorInit,
                                                   yggConfig.batchStoreDelay, context.system.scheduler, yggConfig.batchShutdownCheckInterval) {
          def processMessages(messages: Seq[IngestMessage], batchCoordinator: ActorRef): Unit = {
            implicit val to = yggConfig.metadataTimeout
            implicit val execContext = ExecutionContext.defaultExecutionContext(context.system)
            
            val archivePaths = messages.collect { case ArchiveMessage(_, Archive(path, _)) => path } 
            Future.sequence {
              archivePaths map { path =>
                (metadataActor ? FindDescriptors(path, CPath.Identity)).mapTo[Map[ProjectionDescriptor, ColumnMetadata]]
              }
            }.onSuccess {
              case descMaps : Seq[Map[ProjectionDescriptor, ColumnMetadata]] => ()
                val projectionMap = (for {
                  descMap <- descMaps
                  desc    <- descMap.keys
                  column  <- desc.columns
                } yield (column.path, desc)).groupBy(_._1).mapValues(_.map(_._2))
              
                val updates = routingTable.batchMessages(messages, projectionMap)

                logger.debug("Sending " + updates.size + " update messages")
                batchCoordinator ! ProjectionUpdatesExpected(updates.size)
                for (update <- updates) projectionsActor.tell(update, batchCoordinator)
            }
          }
        }), "ingestRouter")
      }
    }

    def receive = {
      // Route subordinate messages
      case pMsg: ShardProjectionAction => projectionsActor.tell(pMsg, sender)
      case mMsg: ShardMetadataAction   => metadataActor.tell(mMsg, sender)
      case iMsg: ShardIngestAction     => ingestSystem.tell(iMsg, sender)

      case Status => {
        implicit val to = Timeout(yggConfig.statusTimeout)
        implicit val execContext = ExecutionContext.defaultExecutionContext(context.system)
        
        sender ! (for (statusResponses <- Future.sequence { actorsWithStatus map { actor => (actor ? Status).mapTo[JValue] } }) yield JArray(statusResponses))
      }

      case ShutdownSystem => {
        onShutdown()
        sender ! ShutdownComplete
        self ! PoisonPill
      }
    }

    protected def actorsWithStatus = ingestSystem :: metadataActor :: projectionsActor :: Nil

    protected def onShutdown(): Future[Unit] = {
      implicit val execContext = ExecutionContext.defaultExecutionContext(context.system)
      for {
        _ <- Future(logger.info("Stopping shard system"))
        _ <- metadataSync.map(syncJob => Future{ syncJob.cancel() }).getOrElse(Future(()))
        _ <- actorStop(ingestSystem, "ingest")
        _ <- actorStop(projectionsActor, "projections")
        _ <- actorStop(metadataActor, "metadata")
      } yield ()
    }

    protected def actorStop(actor: ActorRef, name: String)(implicit executor: ExecutionContext): Future[Unit] = { 
      for {
        _ <- Future(logger.debug(yggConfig.logPrefix + " Stopping " + name + " actor within " + yggConfig.stopTimeout.duration))
        b <- gracefulStop(actor, yggConfig.stopTimeout.duration)(context.system) 
      } yield {
        logger.debug(yggConfig.logPrefix + " Stop call for " + name + " actor returned " + b)  
      }   
    } recover { 
      case e => logger.error("Error stopping " + name + " actor", e)  
    }   
  }
}
