package com.precog
package ingest
package kafka

import common._

import java.util.concurrent.atomic.AtomicInteger

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

class KafkaEventStore(router: EventRouter, producerId: Int, firstEventId: Int = 0)(implicit dispatcher: MessageDispatcher) extends EventStore {
  private val nextEventId = new AtomicInteger(firstEventId)
  
  def save(event: Event) = {
    val eventId = nextEventId.incrementAndGet
    router.route(EventMessage(producerId, eventId, event)) map { _ => () }
  }

  def close(): Future[Unit] = router.close
}
