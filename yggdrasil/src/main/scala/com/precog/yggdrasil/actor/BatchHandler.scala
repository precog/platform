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
import com.precog.util._
import com.precog.common._

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.dispatch.Promise
import akka.util.Timeout

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.syntax.monoid._

case class BatchComplete(checkpoint: YggCheckpoint, updatedProjections: Seq[(ProjectionDescriptor, Option[ColumnMetadata])])
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
  private var updatedProjections = Seq.empty[(ProjectionDescriptor, Option[ColumnMetadata])]

  override def preStart() = {
    context.system.scheduler.scheduleOnce(ingestTimeout.duration, self, PoisonPill)
  }

  def receive = {
    case ProjectionUpdatesExpected(count) => 
      remaining += (count + 1)
      logger.trace("Should expect %d more updates (total %d)".format(count, remaining))
      if (remaining == 0) self ! PoisonPill

    case InsertMetadata(descriptor, columnMetadata) =>
      logger.trace("Insert metadata complete for " + descriptor)
      updatedProjections = (descriptor, Some(columnMetadata)) +: updatedProjections 
      remaining -= 1
      if (remaining == 0) self ! PoisonPill

    case ArchiveMetadata(descriptor) =>
      logger.info("Archive complete for " + descriptor)
      updatedProjections = (descriptor, None) +: updatedProjections 
      remaining -= 1
      if (remaining == 0) self ! PoisonPill

    case InsertNoMetadata =>
      logger.debug("Insert without metadata")
      remaining -= 1
      if (remaining == 0) self ! PoisonPill
  }

  override def postStop() = {
    // if the ingest isn't complete by the timeout, ask the requestor to retry
    if (remaining != 0) {
      logger.info("Incomplete with %d remaining".format(remaining))
      ingestActor ! BatchFailed(requestor, checkpoint)
    } else {
      // update the metadatabase, by way of notifying the ingest actor
      // so that any pending completions that arrived out of order can be cleared.
      logger.info("Sending complete on batch")
      ingestActor ! BatchComplete(checkpoint, updatedProjections)
    }
  }
}


// vim: set ts=4 sw=4 et:
