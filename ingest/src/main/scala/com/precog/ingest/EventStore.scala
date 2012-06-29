package com.precog.ingest

import util.FutureUtils

import com.precog.common.{Event, EventMessage, IngestMessage}

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
  def save(event: Event, timeout: Timeout): Future[Unit]
  def start: Future[Unit]
  def stop: Future[Unit]
}

trait RouteTable {
  def routeTo(event: Event): NonEmptyList[MailboxAddress]
  def close: Future[Unit]
}

trait Messaging {
  def send(address: MailboxAddress, msg: EventMessage): Future[Unit]
  def close: Future[Unit]
}

trait IngestMessageReceivers {
  def find(address: MailboxAddress): List[IngestMessageReceiver]
}

case class MailboxAddress(id: Long)
//
//class SyncMessages(producerId: Int, initialId: Int = 1) {
//  private val nextId = new AtomicInteger(initialId)
//  
//  val start: SyncMessage = SyncMessage(producerId, 0, List.empty)
//  def next(eventIds: List[Int]): SyncMessage = SyncMessage(producerId, nextId.getAndIncrement(), eventIds)
//  def stop(eventIds: List[Int] = List.empty) = SyncMessage(producerId, Int.MaxValue, eventIds)
//}
//
trait IngestMessageReceiver extends Iterator[IngestMessage] {
  def sync(): Unit
}

class EventRouter(routeTable: RouteTable, messaging: Messaging) {
  def route(msg: EventMessage)(implicit dispatcher: MessageDispatcher): Future[Boolean] = {
    val routes = routeTable.routeTo(msg.event).list
    val futures: List[Future[Unit]] = routes map { messaging.send(_, msg) }

    Future.find(futures)(_ => true) map { _.isDefined }
  }

  def close(implicit dispatcher: MessageDispatcher): Future[Any] = {
    Future.sequence(List(routeTable.close, messaging.close))
  }
}

class ConstantRouteTable(addresses: NonEmptyList[MailboxAddress])(implicit dispather: MessageDispatcher) extends RouteTable {
  def routeTo(event: Event): NonEmptyList[MailboxAddress] = addresses
  def close = Future(())
}

class EchoMessaging(implicit dispatcher: MessageDispatcher) extends Messaging {
  def send(address: MailboxAddress, msg: EventMessage): Future[Unit] = {
    Future(println("Sending: " + msg + " to " + address))
  }

  def close = Future(())
}

class CollectingMessaging(implicit dispatcher: MessageDispatcher) extends Messaging {

  val events = ListBuffer[IngestMessage]()

  def send(address: MailboxAddress, msg: EventMessage): Future[Unit] = {
    events += msg
    Future(())
  }

  def close() = Future(())
}
// vim: set ts=4 sw=4 et:
