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
import com.precog.common.security.PermissionsFinder
import com.precog.common.ingest._
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

// type ShardSystemActor

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
case class IngestSystem(ingestActor: ActorRef, stoppable: Stoppable)

object IngestSystem extends Logging {
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

  protected def initIngestActor(actorSystem: ActorSystem, routingActor: ActorRef, checkpoint: YggCheckpoint, checkpointCoordination: CheckpointCoordination, permissionsFinder: PermissionsFinder[Future]): Option[ActorRef]

  def initShardActors(permissionsFinder: PermissionsFinder[Future], routingActor: ActorRef): Option[IngestSystem] = {
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

    val ingestActor = for (checkpoint <- initialCheckpoint; init <- initIngestActor(ingestActorSystem, routingActor, checkpoint, checkpointCoordination, permissionsFinder)) yield init

    val stoppable = Stoppable.fromFuture({
      import IngestSystem.actorStop
      logger.info("Stopping shard system")
      for {
        _ <- ingestActor map { actorStop(yggConfig, _, "ingestActor")(ingestActorSystem, ingestActorSystem.dispatcher) } getOrElse { Future(())(ingestActorSystem.dispatcher) }
      } yield {
        ingestActorSystem.shutdown()
        logger.info("Shard system stopped.")
      }
    })

    ingestActor map { IngestSystem(_, stoppable) }
  }
}
