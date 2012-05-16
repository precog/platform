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

import annotation.tailrec
import collection.mutable
import java.util.concurrent.atomic.AtomicLong

import scalaz._
import scalaz.syntax.bind._

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

case object Start 
case object ControlledStop
case class AwaitShutdown(replyTo: ActorRef)

class BatchStoreActor(routingDispatch: RoutingDispatch, idleDelay: Duration, ingestActor: Option[ActorRef], scheduler: Scheduler, shutdownCheck: Duration) extends Actor with Logging {
  private val initiated = new AtomicLong(0)
  private val processed = new AtomicLong(0)
  private var inShutdown = false

  private val initialAction = GetIngestNow
  private val delayIngestFollowup = GetIngestAfter(idleDelay)

  def receive = {
    case Status =>
      sender ! status()

    case Start =>
      start
      sender ! ()

    case ControlledStop =>
      initiateShutdown
      self ! AwaitShutdown(sender)

    case as @ AwaitShutdown(replyTo) =>
      val pending = pendingBatches
      if(pending <= 0) {
        logger.debug("Routing actor shutdown - Complete")
        replyTo ! ()
      } else {
        logger.debug("Routing actor shutdown - Pending batches (%d)".format(pending)) 
        scheduler.scheduleOnce(shutdownCheck, self, as)
      }

    case r @ NoIngestData    => processResult(r, sender) 
    case r @ IngestErrors(_) => processResult(r, sender) 
    case r @ IngestData(_)   => processResult(r, sender)

    case DirectIngestData(d) =>
      initiated.incrementAndGet
      processResult(IngestData(d), sender) 

    case m                   => logger.error("Unexpected ingest actor message: " + m.getClass.getName) 
  }

  def status(): JValue = JObject.empty ++ JField("Routing", JObject.empty ++
      JField("initiated", JInt(initiated.get)) ++ JField("processed", JInt(initiated.get)))

  def processResult(ingestResult: IngestResult, replyTo: ActorRef) {
    handleNext(nextAction(ingestResult))
    process(ingestResult, replyTo)
  }

  def pendingBatches(): Long = initiated.get - processed.get

  def scheduleIngestRequest(delay: Duration) {
    for (actor <- ingestActor) {
      initiated.incrementAndGet
      scheduler.scheduleOnce(delay, actor, GetMessages(self))
    }
  }

  def start() {
    inShutdown = false
    handleNext(initialAction)
  }
  
  def initiateShutdown() {
    inShutdown = true
  }
  
  def handleNext(nextAction: IngestAction) {
    if(!inShutdown) {
      nextAction match {
        case GetIngestNow =>
          scheduleIngestRequest(Duration.Zero)
        case GetIngestAfter(delay) =>
          scheduleIngestRequest(delay)
        case NoIngest =>
          // noop
      }
    }
  }

  def nextAction(ingest: IngestResult): IngestAction = ingest match { 
    case NoIngestData => 
      delayIngestFollowup
    case IngestErrors(errors) => 
      logErrors(errors)
      delayIngestFollowup 
    case IngestData(_) => 
      GetIngestNow 
  }

  private implicit val ValidationMonad = Validation.validationMonad[Throwable]
  def process(ingest: IngestResult, replyTo: ActorRef) = ingest match {
    case IngestData(messages) =>
      routingDispatch.storeAll(messages) onComplete { e => 
        processed.incrementAndGet
        sender ! Validation.fromEither(e).join
      }

    case NoIngestData         =>
      processed.incrementAndGet
      // noop

    case IngestErrors(_)      =>
      processed.incrementAndGet
      // noop
  }
  
  private def logErrors(errors: Seq[String]) = errors.foreach( logger.error(_) )
}

case class DirectIngestData(messages: Seq[IngestMessage])

sealed trait IngestAction
case object NoIngest extends IngestAction
case object GetIngestNow extends IngestAction
case class GetIngestAfter(delay: Duration) extends IngestAction
