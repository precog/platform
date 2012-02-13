package com.precog

import common._
import ingest._

import ingest.util.FutureUtils

import com.precog.common.util.ArbitraryIngestMessage

import blueeyes.json.JsonAST._

import akka.actor.ActorSystem
import akka.dispatch.{ExecutionContext, Future, MessageDispatcher}

import scala.collection.mutable.ListBuffer

import scalaz._
import Scalaz._

import java.util.concurrent.atomic.AtomicInteger


package object ingest {

  trait QueryExecutor {
    def execute(query: String): JValue
    def startup: Future[Unit]
    def shutdown: Future[Unit]
  }

  trait NullQueryExecutor extends QueryExecutor {
    def actorSystem: ActorSystem
    implicit def executionContext: ExecutionContext

    def execute(query: String) = JString("Query service not avaialble")
    def startup = Future(())
    def shutdown = Future { actorSystem.shutdown }
  }

  trait EventStore {
    def save(event: Event): Future[Unit]
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

  object TestIngestMessageRecievers extends IngestMessageReceivers with ArbitraryIngestMessage {
    def find(address: MailboxAddress) = List(new IngestMessageReceiver() {
      def hasNext = true
      def next() = genRandomEventMessage.sample.get
      def sync() = Unit
    })  
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
