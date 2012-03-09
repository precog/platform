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

import akka.util.Timeout

import common._
import ingest._

import ingest.util.FutureUtils

import blueeyes.json.JsonAST._

import akka.actor.ActorSystem
import akka.dispatch.{ExecutionContext, Future, MessageDispatcher}

import scala.collection.mutable.ListBuffer

import scalaz._
import Scalaz._

import java.util.concurrent.atomic.AtomicInteger

package object ingest {

  trait EventStore {
    def save(event: Event, timeout: Timeout): Future[Unit]
    def start(): Future[Unit]
    def stop(): Future[Unit]
  }

  trait RouteTable {
    def routeTo(event: Event): NonEmptyList[MailboxAddress]
    def close(): Future[Unit]
  }

  trait Messaging {
    def send(address: MailboxAddress, msg: EventMessage): Future[Unit]
    def close(): Future[Unit]
  }
 
  trait IngestMessageReceivers {
    def find(address: MailboxAddress): List[IngestMessageReceiver]
  }

  case class MailboxAddress(id: Long)

  class SyncMessages(producerId: Int, initialId: Int = 1) {
    private val nextId = new AtomicInteger(initialId)
    
    val start: SyncMessage = SyncMessage(producerId, 0, List.empty)
    def next(eventIds: List[Int]): SyncMessage = SyncMessage(producerId, nextId.getAndIncrement(), eventIds)
    def stop(eventIds: List[Int] = List.empty) = SyncMessage(producerId, Int.MaxValue, eventIds)
  }

  trait IngestMessageReceiver extends Iterator[IngestMessage] {
    def hasNext: Boolean
    def next(): IngestMessage 
    def sync()
  }

  class EventRouter(routeTable: RouteTable, messaging: Messaging) {
    def route(msg: EventMessage)(implicit dispatcher: MessageDispatcher): Future[Option[Unit]] = {
      FutureUtils.singleSuccess( routeTable.routeTo(msg.event).map { messaging.send(_, msg) }.list ).map{ _.map{ _ => () }}
    }

    def close()(implicit dispatcher: MessageDispatcher): Future[Unit] = {
      Future.sequence(List(routeTable.close, messaging.close)).map(_ => ())
    }
  }

  class ConstantRouteTable(addresses: NonEmptyList[MailboxAddress])(implicit dispather: MessageDispatcher) extends RouteTable {
    def routeTo(event: Event): NonEmptyList[MailboxAddress] = addresses
    def close() = Future(())
  }

  class EchoMessaging(implicit dispatcher: MessageDispatcher) extends Messaging {
    def send(address: MailboxAddress, msg: EventMessage): Future[Unit] = {
      Future(println("Sending: " + msg + " to " + address))
    }

    def close() = Future(())
  }

  class CollectingMessaging(implicit dispatcher: MessageDispatcher) extends Messaging {

    val events = ListBuffer[IngestMessage]()

    def send(address: MailboxAddress, msg: EventMessage): Future[Unit] = {
      events += msg
      Future(())
    }

    def close() = Future(())
  }
}
