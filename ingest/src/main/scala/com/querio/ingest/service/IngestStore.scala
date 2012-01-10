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

import akka.dispatch.{Future, Futures}
import akka.dispatch.DefaultCompletableFuture

import blueeyes.json.JPath
import blueeyes.json.JsonAST._

import org.scalacheck.Gen

import kafka.producer._

import com.reportgrid.common._
import com.querio.ingest.api._
import com.querio.ingest.util.ArbitraryJValue

// todo
//
// - write unit test against collecting senders
// - test failure semantics against senders
// - write simple kafka backed senders
//
// - add sync behavior

object FutureHelper {
  def singleSuccess[T](futures: Seq[Future[T]], timeout: Long = Long.MaxValue): Future[T] = new FirstResultFuture(futures, timeout)
  
  class FirstResultFuture[T](futures: Seq[Future[T]], timeout: Long = Long.MaxValue) extends DefaultCompletableFuture[T](timeout) {

    val counter = new AtomicInteger(futures.size)

    def failIfLastChild(t: Throwable) {
      val current = counter.decrementAndGet()
      if (current <= 0 && mayComplete) {
        complete(Left(t))
      }
    }

    def mayComplete(): Boolean = mayComplete(counter.get)

    def mayComplete(cur: Int): Boolean = {
      if(cur == -1) false
      else counter.compareAndSet(cur, -1) || mayComplete(counter.get)
    }

    val childResult: PartialFunction[T, Unit] = {
      case t if mayComplete => complete(Right(t))
    }

    val childException: PartialFunction[Throwable, Unit] = {
      case t => failIfLastChild(t)
    }

    val childTimeout: PartialFunction[Future[T], Unit] = {
      case f => failIfLastChild(new RuntimeException("Akka future timed out."))
    }

    for (f <- futures) {
      f.onResult(childResult)
      f.onException(childException)
      f.onTimeout(childTimeout)
    }
  }
}


class EventStore(router: EventRouter, producerId: Int, firstEventId: Int = 0) {
  
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
  def route(msg: EventMessage): Future[Unit] = {
    FutureHelper.singleSuccess( routeTable.routeTo(msg.event).map { messaging.send(_, msg) }.list )
  }
  def close(): Future[Unit] = {
    Futures.sequence(List(routeTable.close, messaging.close), Long.MaxValue).map(_ => ())
  }
}

class ConstantRouteTable(addresses: NonEmptyList[MailboxAddress]) extends RouteTable {
  def routeTo(event: Event): NonEmptyList[MailboxAddress] = addresses
  def close() = Future(())
}

class EchoMessaging extends Messaging {
  def send(address: MailboxAddress, msg: EventMessage): Future[Unit] = {
    Future(println("Sending: " + msg + " to " + address))
  }

  def close() = Future(())
}

class CollectingMessaging extends Messaging {

  val events = ListBuffer[IngestMessage]()

  def send(address: MailboxAddress, msg: EventMessage): Future[Unit] = {
    events += msg
    Future(())
  }

  def close() = Future(())
}

class SimpleKafkaMessaging(topic: String, config: Properties) extends Messaging {
 
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

