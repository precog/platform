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

import blueeyes.json.JsonAST._

import akka.util.Timeout
import akka.actor.ActorSystem
import akka.dispatch.{ExecutionContext, Future, MessageDispatcher}

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ListBuffer
import scalaz._

trait EventStore {
  // Returns a Future that completes when the storage layer has taken ownership of and
  // acknowledged the receipt of the data.
  def save(action: Action, timeout: Timeout): Future[Unit]
  def start: Future[Unit]
  def stop: Future[Unit]
}

trait RouteTable {
  def routeTo(message: IngestMessage): NonEmptyList[MailboxAddress]
  def close: Future[Unit]
}

trait Messaging {
  def send(address: MailboxAddress, msg: IngestMessage): Future[Unit]
  def close: Future[Unit]
}

case class MailboxAddress(id: Long)

class EventRouter(routeTable: RouteTable, messaging: Messaging) {
  def route(msg: IngestMessage)(implicit dispatcher: MessageDispatcher): Future[Boolean] = {
    val routes = routeTable.routeTo(msg).list
    val futures: List[Future[Unit]] = routes map { messaging.send(_, msg) }

    Future.find(futures)(_ => true) map { _.isDefined }
  }

  def close(implicit dispatcher: MessageDispatcher): Future[Any] = {
    Future.sequence(List(routeTable.close, messaging.close))
  }
}

class ConstantRouteTable(addresses: NonEmptyList[MailboxAddress])(implicit dispather: MessageDispatcher) extends RouteTable {
  def routeTo(msg: IngestMessage): NonEmptyList[MailboxAddress] = addresses
  def close = Future(())
}

class EchoMessaging(implicit dispatcher: MessageDispatcher) extends Messaging {
  def send(address: MailboxAddress, msg: IngestMessage): Future[Unit] = {
    Future(println("Sending: " + msg + " to " + address))
  }

  def close = Future(())
}

class CollectingMessaging(implicit dispatcher: MessageDispatcher) extends Messaging {

  val messages = ListBuffer[IngestMessage]()

  def send(address: MailboxAddress, msg: IngestMessage): Future[Unit] = {
    messages += msg
    Future(())
  }

  def close() = Future(())
}
// vim: set ts=4 sw=4 et:
