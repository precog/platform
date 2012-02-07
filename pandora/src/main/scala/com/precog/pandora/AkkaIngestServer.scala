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
import common.Config
import common.Event
import common.EventMessage
import daze.YggdrasilStorage
import ingest.api._
import ingest.util.QuerioZookeeper
import ingest.service.IngestServer
import ingest.service.EventStore
import yggdrasil.YggConfig
import yggdrasil.shard.YggState
import yggdrasil.shard.YggShard
import yggdrasil.shard.RealYggShard

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

trait AkkaIngestServer extends IngestServer with YggdrasilStorage {
  
  trait AkkaIngestConfig extends YggConfig
 
  val actorSystem = ActorSystem("akka_ingest_server")
  implicit val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
  
  implicit def defaultFutureDispatch: MessageDispatcher
  
  def yggConfig: IO[YggConfig]

  def eventStoreFactory(eventConfig: ConfigMap): EventStore = {
    new EventStore {
      private val idSource = new java.util.concurrent.atomic.AtomicInteger(0)

      def save(event: Event): Future[Unit] = {
        val seqId = idSource.incrementAndGet
        (storage.store(EventMessage(0, seqId, event))).mapTo[Unit]
      }
    }
  }
  
  private val yggShard: IO[YggShard] = yggConfig flatMap { cfg => YggState.restore(cfg.dataDir) map { (cfg, _) } } map { 
    case (cfg, Success(state)) =>
      new RealYggShard {
        val yggState = state 
        val yggConfig = cfg 
      }
    case (cfg, Failure(e)) => sys.error("Error loading shard state from: %s cause:\n".format(cfg.dataDir, e))
  }
  
  lazy val storage: YggShard = yggShard.unsafePerformIO
  
  abstract override def stop = super.stop map { _ =>
    actorSystem.shutdown
  } map { _ =>
    AkkaDefaults.actorSystem.shutdown
  }
}

// vim: set ts=4 sw=4 et:
