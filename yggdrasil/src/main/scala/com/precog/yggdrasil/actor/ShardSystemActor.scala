package com.precog.yggdrasil
package actor

import com.precog.accounts.BasicAccountManager
import com.precog.common.{ Archive, ArchiveMessage, CheckpointCoordination, IngestMessage, Path, YggCheckpoint }
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

  protected def initIngestActor(checkpoint: YggCheckpoint, metadataActor: ActorRef, accountManager: BasicAccountManager[Future]): Option[() => Actor]

  protected def checkpointCoordination: CheckpointCoordination

  class ShardSystemActor(storage: MetadataStorage, accountManager: BasicAccountManager[Future]) extends Actor with Logging {

    // The ingest system consists of the ingest supervisor and ingest actor(s)
    private[this] var ingestSystem: ActorRef            = _
    private[this] var metadataActor: ActorRef           = _
    private[this] var projectionsActor: ActorRef        = _
    private[this] var metadataSync: Option[Cancellable] = None

    private[this] val metadataActorSystem = ActorSystem("Metadata")
    private[this] val projectionActorSystem = ActorSystem("Projections")
    private[this] val ingestActorSystem = ActorSystem("Ingest")

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
      metadataActor = metadataActorSystem.actorOf(Props(new MetadataActor(yggConfig.shardId, storage, checkpointCoordination, initialCheckpoint)), "metadata")

      logger.debug("Initializing ProjectionsActor")
      projectionsActor = projectionActorSystem.actorOf(Props(new ProjectionsActor(yggConfig.maxOpenProjections)), "projections")

      val ingestActorInit: Option[() => Actor] = initialCheckpoint flatMap {
        checkpoint: YggCheckpoint => initIngestActor(checkpoint, metadataActor, accountManager)
      }
 
      ingestSystem     = { 
        logger.debug("Initializing ingest system")
        // Ingest implies a metadata sync
        metadataSync = Some(metadataActorSystem.scheduler.schedule(yggConfig.metadataSyncPeriod, yggConfig.metadataSyncPeriod, metadataActor, FlushMetadata))

        val routingTable = new SingleColumnProjectionRoutingTable

        ingestActorSystem.actorOf(Props(new IngestSupervisor(ingestActorInit,
                                                             yggConfig.batchStoreDelay, ingestActorSystem.scheduler, yggConfig.batchShutdownCheckInterval) {
          def processMessages(messages: Seq[IngestMessage], batchCoordinator: ActorRef): Unit = {
            logger.debug("Beginning processing of %d messages".format(messages.size))
            implicit val to = yggConfig.metadataTimeout
            implicit val execContext = ExecutionContext.defaultExecutionContext(ingestActorSystem)
            
            val archivePaths = messages.collect { case ArchiveMessage(_, Archive(path, _)) => path } 

            if (archivePaths.nonEmpty) {
              logger.debug("Processing archive paths: " + archivePaths)
            } else {
              logger.debug("No archive paths")
            }

            Future.sequence {
              archivePaths map { path =>
                (metadataActor ? FindDescriptors(path, CPath.Identity)).mapTo[Set[ProjectionDescriptor]]
              }
            }.onSuccess {
              case descMaps : Seq[Set[ProjectionDescriptor]] => 
                val projectionMap: Map[Path, Seq[ProjectionDescriptor]] = (for {
                  descMap <- descMaps
                  desc    <- descMap
                  column  <- desc.columns
                } yield (column.path, desc)).groupBy(_._1).mapValues(_.map(_._2))

                projectionMap.foreach { case (p,d) => logger.debug("Archiving %d projections on path %s".format(d.size, p)) }
              
                val updates = routingTable.batchMessages(messages, projectionMap)

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
