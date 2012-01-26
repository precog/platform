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
package com.querio.ingest.service

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

import java.util.concurrent.atomic.AtomicInteger
import java.util.Properties

import scalaz._
import scalaz.Scalaz._

import akka.dispatch.{Future, Futures, Promise}
import akka.dispatch.MessageDispatcher

import blueeyes.json.JPath
import blueeyes.json.JsonAST._

import org.scalacheck.Gen

import kafka.producer._

import com.reportgrid.common._
import com.querio.ingest.api._
import com.reportgrid.common.util.ArbitraryJValue

// todo
//
// - write unit test against collecting senders
// - test failure semantics against senders
// - write simple kafka backed senders
//
// - add sync behavior

object FutureHelper {
  def singleSuccess[T](futures: Seq[Future[T]])(implicit dispatcher: MessageDispatcher): Future[Option[T]] = Future.find(futures){ _ => true }
}

class EventStore(router: EventRouter, producerId: Int, firstEventId: Int = 0)(implicit dispatcher: MessageDispatcher) {
  
  private val nextEventId = new AtomicInteger(firstEventId)
  
  def save(event: Event) = router.route(EventMessage(producerId, nextEventId.incrementAndGet, event))
  def close(): Future[Unit] = router.close
}

trait RouteTable {
  def routeTo(event: Event): NonEmptyList[MailboxAddress]
  def close(): Future[Unit]
}

trait Messaging {
  def send(address: MailboxAddress, msg: EventMessage): Future[Unit]
  def close(): Future[Unit]
}

class EventRouter(routeTable: RouteTable, messaging: Messaging) {
  def route(msg: EventMessage)(implicit dispatcher: MessageDispatcher): Future[Option[Unit]] = {
    FutureHelper.singleSuccess( routeTable.routeTo(msg.event).map { messaging.send(_, msg) }.list ).map{ _.map{ _ => () }}
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

class SimpleKafkaMessaging(topic: String, config: Properties)(implicit dispatcher: MessageDispatcher) extends Messaging {
 
  val producer = new Producer[String, IngestMessage](new ProducerConfig(config))

  def send(address: MailboxAddress, msg: EventMessage) = {
    Future {
      val data = new ProducerData[String, IngestMessage](topic, msg)
      try {
        producer.send(data)
      } catch {
        case x => x.printStackTrace(System.out); throw x
      }
    }
  }

  def close() = {
    Future {
      producer.close()
    }
  }

}

