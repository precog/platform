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

import blueeyes.bkka.AkkaDefaults

import net.lag.configgy.ConfigMap

import scalaz.effect.IO

trait AkkaIngestServer extends IngestServer { self =>
  lazy val actorSystem = ActorSystem("akka_ingest_server")
  implicit lazy val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
  
  implicit def defaultFutureDispatch: MessageDispatcher

  def storage: YggShard
  
  def queryExecutorFactory(configMap: ConfigMap) = new NullQueryExecutor {
    lazy val actorSystem = self.actorSystem
    implicit def executionContext = self.asyncContext
  }

  def eventStoreFactory(eventConfig: ConfigMap): EventStore = {
    new EventStore {
      private val idSource = new java.util.concurrent.atomic.AtomicInteger(0)

      def save(event: Event): Future[Unit] = {
        val seqId = idSource.incrementAndGet
        (storage.store(EventMessage(0, seqId, event))).mapTo[Unit]
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
