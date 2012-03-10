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
package com.precog
package yggdrasil
package actor 

import com.precog.common._

import akka.actor.Actor
import akka.actor.Scheduler
import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._
import akka.util.Duration

import com.weiglewilczek.slf4s._


object RoutingActor {
  private val pathDelimeter = "//"
  private val partDelimeter = "-"

  def toPath(columns: Seq[ColumnDescriptor]): String = {
    columns.map( s => sanitize(s.path.toString)).mkString(pathDelimeter) + pathDelimeter +
    columns.map( s => sanitize(s.selector.toString) + partDelimeter + sanitize(s.valueType.toString)).mkString(partDelimeter)
  }

  def projectionSuffix(descriptor: ProjectionDescriptor): String = ""

  private val whitespace = "//W".r
  def sanitize(s: String) = whitespace.replaceAllIn(s, "_") 

  private[actor] implicit val timeout: Timeout = 30 seconds
}

case object ControlledStop
case class AwaitShutdown(replyTo: ActorRef)

class RoutingActor(routingTable: RoutingTable, ingestActor: Option[ActorRef], projectionActors: ActorRef, metadataActor: ActorRef, scheduler: Scheduler, shutdownCheck: Duration = Duration(1, "second")) extends Actor with Logging {
  
  import RoutingActor._

  private var inShutdown = false

  def receive = {
    
    case ControlledStop =>
      inShutdown = true 
      self ! AwaitShutdown(sender)

    case as @ AwaitShutdown(replyTo) =>
      if(inserted.isEmpty) {
        logger.debug("Routing actor shutdown - Complete")
        replyTo ! ()
      } else {
        logger.debug("Routing actor shutdown - Pending inserts (%d)".format(inserted.size)) 
        scheduler.scheduleOnce(shutdownCheck, self, as)
      }

    case CheckMessages =>
      if(!inShutdown) {
        //logger.debug("Routing Actor - Check Messages")
        ingestActor foreach { actor => actor ! GetMessages(self) }
      }
    
    case NoMessages =>
      //logger.debug("Routing Actor - No Messages")
      scheduleNextCheck
    
    case Messages(messages) =>
      //logger.debug("Routing Actor - Processing Message Batch (%d)".format(messages.size))
      batchProcessMessages(messages)
      sender ! ()
    
    case ic @ InsertComplete(_, _, _, _) =>
      //logger.debug("Insert Complete")
      markInsertComplete(ic)
    
    case ibc @ InsertBatchComplete(inserts) =>
      var i = 0
      while(i < inserts.size) {
        markInsertComplete(inserts(i))
        i += 1
      }

  }

  def scheduleNextCheck {
    if(!inShutdown) {
      scheduler.scheduleOnce(Duration(1, "second"), self, CheckMessages)
    }
  }

  def batchProcessMessages(messages: Seq[IngestMessage]) {
    import scala.collection.mutable

    var actions = mutable.Map.empty[ProjectionDescriptor, (Seq[ProjectionInsert], Seq[InsertComplete])]

    var m = 0
    while(m < messages.size) {
      messages(m) match {
        case SyncMessage(_, _, _) => // TODO

        case em @ EventMessage(eventId, _) =>

          val projectionUpdates = routingTable.route(em)
          markInsertsPending(eventId, projectionUpdates.size)  

          var i = 0
          while(i < projectionUpdates.size) {
            val update = projectionUpdates(i)
            val insert = ProjectionInsert(update.identities, update.values)
            val complete = InsertComplete(eventId, update.descriptor, update.values, update.metadata)
            val (inserts, completes) = actions.get(update.descriptor) getOrElse { (Vector.empty, Vector.empty) }
            val newActions = (inserts :+ insert, completes :+ complete) 
            actions += (update.descriptor -> newActions)
            i += 1
          }
      }
      m += 1
    }

    val acquire = projectionActors ? AcquireProjectionBatch(actions.keys)
    acquire.onComplete { 
      case Left(t) =>
        logger.error("Exception acquiring projection actor: ", t)

      case Right(ProjectionBatchAcquired(actorMap)) => 
        
        val descItr = actions.keys.iterator
        while(descItr.hasNext) {
          val desc = descItr.next
          val (inserts, completes) = actions(desc)
          val actor = actorMap(desc)
          val fut = actor ? ProjectionBatchInsert(inserts)
          fut.onComplete { _ => 
            self ! InsertBatchComplete(completes)
            projectionActors ! ReleaseProjection(desc)
          }
        }
        
      case Right(ProjectionError(errs)) =>
        for(err <- errs.list) logger.error("Error acquiring projection actor: ", err)
    }
  }

  def processMessages(messages: Seq[IngestMessage]) {
    var m = 0
    while(m < messages.size) {
      messages(m) match {
        case SyncMessage(_, _, _) => // TODO

        case em @ EventMessage(eventId, _) =>

          val projectionUpdates = routingTable.route(em)

          markInsertsPending(eventId, projectionUpdates.size)  

          var cnt = 0
          while(cnt < projectionUpdates.length) {
            val pd = projectionUpdates(cnt)
            val acquire = projectionActors ? AcquireProjection(pd.descriptor)
            acquire.onComplete { 
              case Left(t) =>
                logger.error("Exception acquiring projection actor: ", t)

              case Right(ProjectionAcquired(proj)) => 
                val fut = proj ? ProjectionInsert(pd.identities, pd.values)
                fut.onComplete { _ => 
                  self ! InsertComplete(eventId, pd.descriptor, pd.values, pd.metadata)
                  projectionActors ! ReleaseProjection(pd.descriptor)
                }
                
              case Right(ProjectionError(errs)) =>
                for(err <- errs.list) logger.error("Error acquiring projection actor: ", err)
            }
            cnt += 1
          }
      }
      m += 1
    }
  }

  import scala.collection.mutable

  private var expectation = mutable.Map[EventId, Int]()
  private val inserted = mutable.Map[EventId, mutable.ListBuffer[InsertComplete]]()

  def markInsertsPending(eventId: EventId, expected: Int) { 
    expectation += (eventId -> expected)
  }

  def markInsertComplete(insert: InsertComplete) { 
    val eventId = insert.eventId
    val inserts = inserted.get(eventId) map { _ += insert } getOrElse(mutable.ListBuffer(insert))
    if(inserts.size >= expectation(eventId)) {
      expectation -= eventId
      inserted -= eventId
      //logger.debug("Event insert complete: updating metadata")
      metadataActor ! UpdateMetadata(inserts)
    } else {
      inserted += (eventId -> inserts) 
    }
    if(expectation.isEmpty) {
      //logger.debug("Batch complete")
      self ! CheckMessages
    }
  }
}

case class InsertComplete(eventId: EventId, descriptor: ProjectionDescriptor, values: Seq[CValue], metadata: Seq[Set[Metadata]])
case class InsertBatchComplete(inserts: Seq[InsertComplete])
