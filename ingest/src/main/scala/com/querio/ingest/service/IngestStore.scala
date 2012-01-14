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

