package com.precog
package pandora

import analytics.TokenManager
import common.Event
import common.EventMessage
import daze.YggdrasilStorage
import ingest.api._
import ingest.util.QuerioZookeeper
import ingest.service.IngestServer
import ingest.service.EventStore
import yggdrasil.shard.StorageShard
import yggdrasil.shard.ShardConfig
import yggdrasil.shard.FilesystemBootstrapStorageShard

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.dispatch.Await
import akka.pattern.ask
import akka.util.Duration

import blueeyes.BlueEyesServer
import blueeyes.json.JsonAST._
import blueeyes.persistence.mongo.Mongo
import blueeyes.persistence.mongo.MongoCollection
import blueeyes.persistence.mongo.Database
import blueeyes.util.Clock

import java.util.Properties
import java.util.Date
import net.lag.configgy.ConfigMap

import scalaz.NonEmptyList
import scalaz.effect.IO

trait AkkaIngestServer extends IngestServer with YggdrasilStorage {
  val actorSystem = ActorSystem("Pandora REPL")
  implicit val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
 
  def storageShardConfig() = {
    val config = new Properties()
    config.setProperty("precog.storage.root", "/tmp/repl_test_storage") 
    config
  }

  private val storageShard: IO[FilesystemBootstrapStorageShard] = {
    ShardConfig.fromProperties(storageShardConfig) map {
      case scalaz.Success(config) => new FilesystemBootstrapStorageShard {
        val shardConfig = config
      }

      case scalaz.Failure(e) => sys.error("Error loading shard config: " + e)
    }
  }

  lazy val storage: FilesystemBootstrapStorageShard = storageShard.unsafePerformIO

  def eventStoreFactory(eventConfig: ConfigMap): EventStore = {
    new EventStore {
      private val idSource = new java.util.concurrent.atomic.AtomicInteger(0)

      def save(event: Event): Future[Unit] = {
        (storage.routingActor ? EventMessage(0, idSource.incrementAndGet, event)).mapTo[Unit]
      }
    }
  }

  abstract override def stop = super.stop map { _ =>
    actorSystem.shutdown
  }
}


// vim: set ts=4 sw=4 et:
