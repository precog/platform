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
  val actorSystem = ActorSystem("akka_ingest_server")
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
