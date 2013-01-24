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

import com.precog.common.{ CheckpointCoordination, YggCheckpoint, Path }
import com.precog.common.accounts.AccountFinder
import com.precog.common.ingest._
import com.precog.common.json._
import com.precog.util.FilesystemFileOps
import com.precog.yggdrasil.metadata.{ ColumnMetadata, FileMetadataStorage, MetadataStorage }

import akka.actor._
import akka.dispatch._
import akka.util._
import akka.util.duration._
import akka.pattern.ask
import akka.pattern.gracefulStop

import blueeyes.json._

import com.weiglewilczek.slf4s.Logging
import org.streum.configrity.converter.Extra._

import scalaz.{Failure,Success}
import scalaz.syntax.applicative._
import scalaz.syntax.semigroup._
import scalaz.syntax.std.boolean._
import scalaz.std.option._
import scalaz.std.map._
import scalaz.std.vector._

case object ShutdownSystem
case object ShutdownComplete

trait ShardConfig extends BaseConfig {
  type IngestConfig

  def shardId: String
  def logPrefix: String

  def ingestConfig: Option[IngestConfig]  

  def statusTimeout: Long = config[Long]("actors.status.timeout", 30000)
  def metadataTimeout: Timeout = config[Long]("actors.metadata.timeout", 30) seconds
  def stopTimeout: Timeout = config[Long]("actors.stop.timeout", 300) seconds

  def maxOpenProjections: Int = config[Int]("actors.store.max_open_projections", 5000)

  def metadataSyncPeriod: Duration = config[Int]("actors.metadata.sync_minutes", 1) minutes
  def metadataPreload: Boolean = config[Boolean]("actors.metadata.preload", true)
  def batchStoreDelay: Duration    = config[Long]("actors.store.idle_millis", 1000) millis
  def batchShutdownCheckInterval: Duration = config[Int]("actors.store.shutdown_check_seconds", 1) seconds
}

trait ShardSystemActorModule extends ProjectionsActorModule with YggConfigComponent {
  type YggConfig <: ShardConfig

  protected def metadataStorage: MetadataStorage
  protected def accountFinder: Option[AccountFinder[Future]]

  protected def initIngestActor(checkpoint: YggCheckpoint, metadataActor: ActorRef, accountFinder: AccountFinder[Future]): Option[() => Actor]

  protected def checkpointCoordination: CheckpointCoordination

  class ShardSystemActor extends Actor with Logging {
    // The ingest system consists of the ingest supervisor and ingest actor(s)
    private[this] var ingestSystem: ActorRef            = _
    private[this] var metadataActor: ActorRef           = _
    private[this] var projectionsActor: ActorRef        = _
    private[this] var metadataSync: Option[Cancellable] = None

    private[this] val metadataActorSystem = ActorSystem("Metadata")
    private[this] val projectionActorSystem = ActorSystem("Projections")
    private[this] val ingestActorSystem = ActorSystem("Ingest")

    private def loadCheckpoint() : Option[YggCheckpoint] = yggConfig.ingestConfig flatMap { _ =>
        checkpointCoordination.loadYggCheckpoint(yggConfig.shardId) match {
          case Some(Failure(errors)) =>
            logger.error("Unable to load Kafka checkpoint: " + errors)
            sys.error("Unable to load Kafka checkpoint: " + errors)

          case Some(Success(checkpoint)) => Some(checkpoint)
          case None => None
        }
      }

    override def preStart() {
      val initialCheckpoint = loadCheckpoint()

      logger.info("Initializing MetadataActor with storage = " + metadataStorage)
      metadataActor = metadataActorSystem.actorOf(Props(new MetadataActor(yggConfig.shardId, metadataStorage, checkpointCoordination, initialCheckpoint, yggConfig.metadataPreload)), "metadata")

      logger.debug("Initializing ProjectionsActor")
      projectionsActor = projectionActorSystem.actorOf(Props(new ProjectionsActor(yggConfig.maxOpenProjections)), "projections")

      val ingestActorInit: Option[() => Actor] = 
        for {
          checkpoint <- initialCheckpoint
          finder <- accountFinder
          init <-  initIngestActor(checkpoint, metadataActor, finder)
        } yield init
 
      ingestSystem     = { 
        logger.debug("Initializing ingest system")
        // Ingest implies a metadata sync
        metadataSync = Some(metadataActorSystem.scheduler.schedule(yggConfig.metadataSyncPeriod, yggConfig.metadataSyncPeriod, metadataActor, FlushMetadata))

        val routingTable = new SingleColumnProjectionRoutingTable

        ingestActorSystem.actorOf(Props(new IngestSupervisor(ingestActorInit,
                                                             yggConfig.batchStoreDelay, ingestActorSystem.scheduler, yggConfig.batchShutdownCheckInterval) {
          //TODO: This needs review; not sure why only archive paths are being considered.
          def processMessages(messages: Seq[EventMessage], batchCoordinator: ActorRef): Unit = {
            logger.debug("Beginning processing of %d messages".format(messages.size))
            implicit val to = yggConfig.metadataTimeout
            implicit val execContext = ExecutionContext.defaultExecutionContext(ingestActorSystem)
            
            //TODO: Make sure that authorization has been checked here.
            val archivePaths = messages.collect { case ArchiveMessage(_, Archive(_, path, jobId)) => path } 

            if (archivePaths.nonEmpty) {
              logger.debug("Processing archive paths: " + archivePaths)
            } else {
              logger.debug("No archive paths")
            }

            Future.sequence {
              archivePaths map { path =>
                (metadataActor ? FindDescriptors(path, CPath.Identity)).mapTo[Set[ProjectionDescriptor]]
              }
            } onSuccess {
              case descMaps : Seq[Set[ProjectionDescriptor]] => 
                val grouped: Map[Path, Seq[ProjectionDescriptor]] = descMaps.flatten.foldLeft(Map.empty[Path, Vector[ProjectionDescriptor]]) {
                  case (acc, descriptor) => descriptor.columns.map(c => (c.path -> Vector(descriptor))).toMap |+| acc
                }
                
                val updates = routingTable.batchMessages(messages, grouped)

                logger.debug("Sending " + updates.size + " update message(s)")
                batchCoordinator ! ProjectionUpdatesExpected(updates.size)
                for (update <- updates) projectionsActor.tell(update, batchCoordinator)
            }
          }
        }), "ingestRouter")
      }
    }

    def receive = {
      // Route subordinate messages
      case pMsg: ShardProjectionAction => 
        logger.trace("Forwarding message " + pMsg + " to projectionsActor")
        projectionsActor.tell(pMsg, sender)
        logger.trace("Forwarding complete: " + pMsg)

      case mMsg: ShardMetadataAction   => 
        logger.trace("Forwarding message " + mMsg + " to metadataActor")
        metadataActor.tell(mMsg, sender)
        logger.trace("Forwarding complete: " + mMsg)

      case iMsg: ShardIngestAction     => 
        logger.trace("Forwarding message " + iMsg + " to ingestSystem")
        ingestSystem.tell(iMsg, sender)
        logger.trace("Forwarding complete: " + iMsg)

      case Status => 
        logger.trace("Processing status request")
        implicit val to = Timeout(yggConfig.statusTimeout)
        implicit val execContext = ExecutionContext.defaultExecutionContext(context.system)
        (for (statusResponses <- Future.sequence { actorsWithStatus map { actor => (actor ? Status).mapTo[JValue] } }) yield JArray(statusResponses)) onSuccess {
          case status => 
            sender ! status
      }
        logger.trace("Status request complete")

      case ShutdownSystem => 
        logger.info("Initiating shutdown")
        onShutdown()
        sender ! ShutdownComplete
        self ! PoisonPill
        logger.info("Shutdown complete")

      case bad =>
        logger.error("Unknown message received: " + bad)
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
