package com.precog
package pandora

import analytics.TokenManager
import common.Config
import common.Event
import common.EventMessage
import ingest.api._
import ingest.util.QuerioZookeeper
import ingest.service.IngestServer
import ingest.service.EventStore
import yggdrasil.YggConfig
import yggdrasil.shard.YggState
import yggdrasil.shard.YggShard
import yggdrasil.shard.ActorYggShard

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.dispatch.Await
import akka.dispatch.MessageDispatcher
import akka.pattern.ask
import akka.util.Duration

import blueeyes.bkka.AkkaDefaults
import blueeyes.BlueEyesServer
import blueeyes.json.JsonAST._
import blueeyes.persistence.mongo.Mongo
import blueeyes.persistence.mongo.MongoCollection
import blueeyes.persistence.mongo.Database
import blueeyes.util.Clock

import java.io.File
import java.util.{Date, Properties}

import net.lag.configgy.ConfigMap

import scalaz.{NonEmptyList, Validation, Success, Failure}
import scalaz.effect.IO

trait AkkaIngestServer extends IngestServer {
  trait AkkaIngestConfig extends YggConfig
 
  lazy val actorSystem = ActorSystem("akka_ingest_server")
  implicit lazy val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
  
  implicit def defaultFutureDispatch: MessageDispatcher

  def storage: YggShard
  
  def eventStoreFactory(eventConfig: ConfigMap): EventStore = {
    new EventStore {
      private val idSource = new java.util.concurrent.atomic.AtomicInteger(0)

      def save(event: Event): Future[Unit] = {
        val seqId = idSource.incrementAndGet
        (storage.store(EventMessage(0, seqId, event))).mapTo[Unit]
      }
    }
  }
  
  abstract override def stop = super.stop map { _ =>
    actorSystem.shutdown
  } map { _ =>
    AkkaDefaults.actorSystem.shutdown
  }
}

// vim: set ts=4 sw=4 et:
