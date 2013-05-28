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

import metadata.ColumnMetadata
import metadata.ColumnMetadata._
import vfs.UpdateSuccess
import com.precog.util._
import com.precog.common._
import com.precog.yggdrasil.vfs._

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.dispatch.Promise
import akka.util.Timeout

import com.weiglewilczek.slf4s.Logging

import scalaz.{NonEmptyList => NEL, _}
import scalaz.syntax.monoid._

case class BatchComplete(requestor: ActorRef, checkpoint: YggCheckpoint)
case class BatchFailed(requestor: ActorRef, checkpoint: YggCheckpoint)

class BatchCompleteNotifier(p: Promise[BatchComplete]) extends Actor {
  def receive = {
    case complete : BatchComplete =>
      p.complete(Right(complete))
      self ! PoisonPill

    case other =>
      p.complete(Left(new RuntimeException("Received non-complete notification: " + other.toString)))
      self ! PoisonPill
  }
}


/**
 * A batch handler actor is responsible for tracking confirmation of persistence for
 * all the messages in a specific batch. It sends
 */
class BatchHandler(ingestActor: ActorRef, requestor: ActorRef, checkpoint: YggCheckpoint, ingestTimeout: Timeout) extends Actor with Logging {
  private var remaining = -1

  override def preStart() = {
    logger.debug("Starting new BatchHandler reporting to " + requestor)
    context.system.scheduler.scheduleOnce(ingestTimeout.duration, self, PoisonPill)
  }

  def receive = {
    case ProjectionUpdatesExpected(count) =>
      remaining += (count + 1)
      logger.trace("Should expect %d more updates (total %d)".format(count, remaining))
      if (remaining == 0) self ! PoisonPill

    case InsertComplete(path) =>
      logger.trace("Insert complete for " + path)
      remaining -= 1
      if (remaining == 0) self ! PoisonPill

    case UpdateSuccess(path) =>
      logger.trace("Update complete for " + path)
      remaining -= 1
      if (remaining == 0) self ! PoisonPill

      // These next two cases are errors that should not terminate the batch
    case UpdateFailure(path, NEL(ResourceError.PermissionsError(msg))) =>
      logger.warn("Permissions failure on %s: %s".format(path, msg))
      remaining -= 1
      if (remaining == 0) self ! PoisonPill

    case UpdateFailure(path, NEL(ResourceError.IllegalWriteRequestError(msg))) =>
      logger.warn("Illegal write failure on %s: %s".format(path, msg))
      remaining -= 1
      if (remaining == 0) self ! PoisonPill

    case UpdateFailure(path, errors) =>
      logger.error("Failure during batch update on %s:\n  %s".format(path, errors.list.mkString("\n  ")))
      self ! PoisonPill

    case ArchiveComplete(path) =>
      logger.info("Archive complete for " + path)
      remaining -= 1
      if (remaining == 0) self ! PoisonPill

    case other =>
      logger.warn("Received unexpected message in BatchHandler: " + other)
  }

  override def postStop() = {
    // if the ingest isn't complete by the timeout, ask the requestor to retry
    if (remaining != 0) {
      logger.info("Sending incomplete with %d remaining to %s".format(remaining, requestor))
      ingestActor ! BatchFailed(requestor, checkpoint)
    } else {
      // update the metadatabase, by way of notifying the ingest actor
      // so that any pending completions that arrived out of order can be cleared.
      logger.info("Sending complete on batch to " + requestor)
      ingestActor ! BatchComplete(requestor, checkpoint)
    }
  }
}


// vim: set ts=4 sw=4 et:
