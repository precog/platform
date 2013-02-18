package com.precog.yggdrasil
package actor

import com.precog.common.{ CheckpointCoordination, YggCheckpoint, Path }
import com.precog.common.accounts.AccountFinder
import com.precog.common.ingest._
import com.precog.common.json._
import com.precog.util.FilesystemFileOps

import akka.actor._
import akka.dispatch._
import akka.util._
import akka.util.duration._
import akka.pattern.ask
import akka.pattern.gracefulStop

import blueeyes.json._
import blueeyes.bkka.Stoppable

import com.weiglewilczek.slf4s._
import org.streum.configrity.converter.Extra._

import scalaz.{Failure,Success}
import scalaz.syntax.applicative._
import scalaz.syntax.semigroup._
import scalaz.syntax.std.boolean._
import scalaz.std.option._
import scalaz.std.map._
import scalaz.std.vector._

trait ShardConfig extends BaseConfig {
  type IngestConfig

  def shardId: String
  def logPrefix: String

  def ingestConfig: Option[IngestConfig]  

  def statusTimeout: Long = config[Long]("actors.status.timeout", 30000)
  def stopTimeout: Timeout = config[Long]("actors.stop.timeout", 300) seconds

  def batchStoreDelay: Duration    = config[Long]("actors.store.idle_millis", 1000) millis
  def batchShutdownCheckInterval: Duration = config[Int]("actors.store.shutdown_check_seconds", 1) seconds
}

// The ingest system consists of the ingest supervisor and ingest actor(s)
case class ShardActors(ingestSystem: ActorRef, stoppable: Stoppable)

object ShardActors extends Logging {
  def actorStop(config: ShardConfig, actor: ActorRef, name: String)(implicit system: ActorSystem, executor: ExecutionContext): Future[Unit] = { 
    for {
      _ <- Future(logger.debug(config.logPrefix + " Stopping " + name + " actor within " + config.stopTimeout.duration))
      b <- gracefulStop(actor, config.stopTimeout.duration)
    } yield {
      logger.debug(config.logPrefix + " Stop call for " + name + " actor returned " + b)  
    }   
  } recover { 
    case e => logger.error("Error stopping " + name + " actor", e)  
  }   
}

trait ShardSystemActorModule extends YggConfigComponent with Logging {
  type YggConfig <: ShardConfig

  protected def checkpointCoordination: CheckpointCoordination

  protected def initIngestActor(actorSystem: ActorSystem, checkpoint: YggCheckpoint, checkpointCoordination: CheckpointCoordination, accountFinder: AccountFinder[Future]): Option[ActorRef]

  def initShardActors(accountFinder: AccountFinder[Future], projectionsActor: ActorRef): ShardActors = {
    //FIXME: move outside
    val ingestActorSystem: ActorSystem = ActorSystem("Ingest")

    def loadCheckpoint() : Option[YggCheckpoint] = yggConfig.ingestConfig flatMap { _ =>
      checkpointCoordination.loadYggCheckpoint(yggConfig.shardId) match {
        case Some(Failure(errors)) =>
          logger.error("Unable to load Kafka checkpoint: " + errors)
          sys.error("Unable to load Kafka checkpoint: " + errors)

        case Some(Success(checkpoint)) => Some(checkpoint)
        case None => None
      }
    }

    val initialCheckpoint = loadCheckpoint()

    val ingestActor = for (checkpoint <- initialCheckpoint; init <- initIngestActor(ingestActorSystem, checkpoint, checkpointCoordination, accountFinder)) yield init

    logger.debug("Initializing ingest system")
    val ingestSystem = ingestActorSystem.actorOf(Props(
      new IngestSupervisor(ingestActor, projectionsActor, ingestActorSystem.scheduler, yggConfig.batchStoreDelay, yggConfig.batchShutdownCheckInterval)
      ), 
      "ingestRouter"
    )

    val stoppable = Stoppable.fromFuture({
      import ShardActors.actorStop
      logger.info("Stopping shard system")
      for {
        _ <- ingestActor map { actorStop(yggConfig, _, "ingestActor")(ingestActorSystem, ingestActorSystem.dispatcher) } getOrElse { Future(())(ingestActorSystem.dispatcher) }
        _ <- actorStop(yggConfig, ingestSystem, "ingestSupervisor")(ingestActorSystem, ingestActorSystem.dispatcher)
      } yield {
        ingestActorSystem.shutdown() 
        logger.info("Shard system stopped.")
      }
    })

    ShardActors(ingestSystem, stoppable)
  }
}
