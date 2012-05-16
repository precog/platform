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

import com.precog.common._

import akka.actor.Actor
import akka.actor.Scheduler
import akka.actor.ActorRef
import akka.dispatch.Await
import akka.dispatch.Future
import akka.dispatch.ExecutionContext
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._
import akka.util.Duration

import blueeyes.json.JsonAST._

import com.weiglewilczek.slf4s._

import java.util.concurrent.atomic.AtomicLong
import scala.annotation.tailrec
import scala.collection.mutable
import scalaz._

case class InsertComplete(eventId: EventId, descriptor: ProjectionDescriptor, values: Seq[CValue], metadata: Seq[Set[Metadata]])
case class InsertBatchComplete(inserts: Seq[InsertComplete])

class EventStore(routingTable: RoutingTable, projectionActors: ActorRef, metadataActor: ActorRef, batchTimeout: Duration)(implicit timeout: Timeout, execContext: ExecutionContext) extends Logging {
  type ActionMap = mutable.Map[ProjectionDescriptor, (Seq[ProjectionInsert], Seq[InsertComplete])]

  def store(events: Seq[IngestMessage]): Future[Validation[Throwable, Unit]] = {
    import scala.collection.mutable

    var actionMap = buildActions(events)
    dispatchActions(actionMap)
  }

  def buildActions(events: Seq[IngestMessage]): ActionMap = {
    def updateActions(event: IngestMessage, actions: ActionMap): ActionMap = {
      event match {
        case SyncMessage(_, _, _) => actions 

        case em @ EventMessage(eventId, _) =>

          @tailrec
          def update(updates: Array[ProjectionData], actions: ActionMap, i: Int = 0): ActionMap = {

            def applyUpdates(update: ProjectionData, actions: ActionMap): ActionMap = {
              val insert = ProjectionInsert(update.identities, update.values)
              val complete = InsertComplete(eventId, update.descriptor, update.values, update.metadata)
              val (inserts, completes) = actions.get(update.descriptor) getOrElse { (Vector.empty, Vector.empty) }
              val newActions = (inserts :+ insert, completes :+ complete) 
              actions += (update.descriptor -> newActions)
            }

            if(i < updates.length) {
              update(updates, applyUpdates(updates(i), actions), i+1)
            } else {
              actions
            }
          }

          update(routingTable.route(em), actions)
      }
    }
  
    @tailrec
    def build(events: Seq[IngestMessage], actions: ActionMap, i: Int = 0): ActionMap = {
      if(i < events.length) {
        build(events, updateActions(events(i), actions), i+1)
      } else {
        actions
      }
    }

    build(events, mutable.Map.empty[ProjectionDescriptor, (Seq[ProjectionInsert], Seq[InsertComplete])])
  }

  val batchCounter = new AtomicLong(0)

  def dispatchActions(actions: ActionMap): Future[Validation[Throwable, Unit]] = {
    logger.info("Pending batches: " + batchCounter.get)
    val acquire = projectionActors ? AcquireProjectionBatch(actions.keys)

    acquire.map { x => batchCounter.incrementAndGet; x }.flatMap {
      case ProjectionBatchAcquired(actorMap) =>
        for {
          descs <-  Future.sequence {
                      actions.keys map { desc =>
                        val (inserts, completes)  = actions(desc)
                        val actor = actorMap(desc)
                        for {
                          _ <- actorMap(desc) ? ProjectionBatchInsert(inserts)
                          _ <- metadataActor  ? UpdateMetadata(completes)
                        } yield desc
                      }
                    }

          _     <-  projectionActors ? ReleaseProjectionBatch(descs.toSeq)
        } yield {
          batchCounter.decrementAndGet 
          Success(()) 
        }

      case ProjectionError(errs) =>
        Future(Failure(errs))
    }
  }
}
// vim: set ts=4 sw=4 et:
