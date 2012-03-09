package com.precog
package pandora

import common.Event
import common.EventMessage
import common.NullQueryExecutor
import ingest.EventStore
import ingest.IngestServer
import yggdrasil.shard.YggShard

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.dispatch.MessageDispatcher
import akka.util.Timeout

import blueeyes.bkka.AkkaDefaults

import scalaz.effect.IO

import org.streum.configrity.Configuration

trait AkkaIngestServer extends IngestServer { self =>
  lazy val actorSystem = ActorSystem("akka_ingest_server")
  implicit lazy val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
  
  implicit def defaultFutureDispatch: MessageDispatcher

  def storage: YggShard
  
  def queryExecutorFactory(config: Configuration) = new NullQueryExecutor {
    lazy val actorSystem = self.actorSystem
    implicit def executionContext = self.asyncContext
  }

  def eventStoreFactory(eventConfig: Configuration): EventStore = {
    new EventStore {
      private val idSource = new java.util.concurrent.atomic.AtomicInteger(0)

      def save(event: Event, timeout: Timeout): Future[Unit] = {
        val seqId = idSource.incrementAndGet
        (storage.store(EventMessage(0, seqId, event), timeout)).mapTo[Unit]
      }

      def start() = Future { () }(asyncContext)
      def stop() = Future { () }(asyncContext)
    }
  }
  
  abstract override def stop = super.stop map { _ =>
    actorSystem.shutdown
  } map { _ =>
    AkkaDefaults.actorSystem.shutdown
  }
}

// vim: set ts=4 sw=4 et:
