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
package com.precog.ingest
package kafka

import com.precog.common._
import com.precog.common.jobs._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.ingest.service._
import WebJobManager._

import blueeyes.BlueEyesServer
import blueeyes.bkka._
import blueeyes.util.Clock

import akka.util.Timeout
import akka.dispatch.{ ExecutionContext, Future }

import org.streum.configrity.Configuration

import scalaz._

object KafkaEventServer extends BlueEyesServer with KafkaEventStoreComponent with AkkaDefaults {
  val clock = Clock.System
  implicit val executionContext = defaultFutureDispatch
  implicit val M: Monad[Future] = new FutureMonad(defaultFutureDispatch)
}


trait KafkaEventStoreComponent extends EventService {
  def APIKeyFinder(config: Configuration) = WebAPIKeyFinder(config)
  def AccountFinder(config: Configuration) = WebAccountFinder(config)
  def JobManager(config: Configuration) = WebJobManager(config).withM[Future]
  def EventStore(config: Configuration): EventStore = {
    val centralZookeeperHosts = getConfig(config, "central.zk.connect")

    val serviceUID = ZookeeperSystemCoordination.extractServiceUID(config)
    val coordination = ZookeeperSystemCoordination(centralZookeeperHosts, serviceUID, true)
    val agent = serviceUID.hostId + serviceUID.serviceId  

    val localConfig = config.detach("local")

    val eventIdSeq = new SystemEventIdSequence(agent, coordination)
    val accountFinder = AccountFinder(config.detach("accountFinder"))
    val eventStore = new LocalKafkaEventStore(localConfig)
    val relayAgent = new KafkaRelayAgent(accountFinder, eventIdSeq, localConfig, config.detach("central"))

    new EventStore {
      def save(action: Event, timeout: Timeout) = eventStore.save(action, timeout)
      def start() = relayAgent.start flatMap { _ => eventStore.start }
      def stop() = eventStore.stop flatMap { _ => relayAgent.stop }
    }
  }

  def getConfig(cfg: Configuration, key: String): String = cfg.get[String](key) getOrElse {
    sys.error("Invalid configuration eventStore.%s required".format(key))
  }
}

