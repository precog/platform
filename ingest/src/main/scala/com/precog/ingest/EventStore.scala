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
package com.precog.ingest

import util.FutureUtils

import com.precog.common.{ Action, IngestMessage }
import com.precog.util.PrecogUnit

import blueeyes.json._

import akka.util.Timeout
import akka.actor.ActorSystem
import akka.dispatch.{ExecutionContext, Future, MessageDispatcher, Promise}

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ListBuffer
import scalaz._

trait EventStore {
  // Returns a Future that completes when the storage layer has taken ownership of and
  // acknowledged the receipt of the data.
  def save(action: Action, timeout: Timeout): Future[PrecogUnit]
  def start: Future[PrecogUnit]
  def stop: Future[PrecogUnit]
}

trait RouteTable {
  def routeTo(message: IngestMessage): NonEmptyList[MailboxAddress]
  def close: Future[PrecogUnit]
}

trait Messaging {
  def send(address: MailboxAddress, msg: IngestMessage): Future[PrecogUnit]
  def close: Future[PrecogUnit]
}

case class MailboxAddress(id: Long)

class EventRouter(routeTable: RouteTable, messaging: Messaging) {
  def route(msg: IngestMessage)(implicit dispatcher: MessageDispatcher): Future[Boolean] = {
    val routes = routeTable.routeTo(msg).list
    val futures: List[Future[PrecogUnit]] = routes map { messaging.send(_, msg) }

    Future.find(futures)(_ => true) map { _.isDefined }
  }

  def close(implicit dispatcher: MessageDispatcher): Future[Any] = {
    Future.sequence(List(routeTable.close, messaging.close))
  }
}

class ConstantRouteTable(addresses: NonEmptyList[MailboxAddress])(implicit dispather: MessageDispatcher) extends RouteTable {
  def routeTo(msg: IngestMessage): NonEmptyList[MailboxAddress] = addresses
  def close = Promise.successful(PrecogUnit)
}

class EchoMessaging(implicit dispatcher: MessageDispatcher) extends Messaging {
  def send(address: MailboxAddress, msg: IngestMessage): Future[PrecogUnit] = {
    println("Sending: " + msg + " to " + address)
    Promise.successful(PrecogUnit)
  }

  def close = Promise.successful(PrecogUnit)
}

class CollectingMessaging(implicit dispatcher: MessageDispatcher) extends Messaging {

  val messages = ListBuffer[IngestMessage]()

  def send(address: MailboxAddress, msg: IngestMessage): Future[PrecogUnit] = {
    messages += msg
    Promise.successful(PrecogUnit)
  }

  def close() = Promise.successful(PrecogUnit)
}
// vim: set ts=4 sw=4 et:
