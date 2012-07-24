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

import blueeyes.bkka.AkkaDefaults
import blueeyes.BlueEyesServer
import blueeyes.util.Clock

import akka.util.Timeout
import akka.dispatch.MessageDispatcher

import com.precog.common._
import com.precog.common.security._
import com.precog.ingest.service._
import com.precog.common.security._

import java.util.Properties

import java.net.InetAddress

import com.weiglewilczek.slf4s.Logging

import org.streum.configrity.Configuration

object KafkaIngestServer extends 
    BlueEyesServer with 
    IngestService with 
    MongoTokenManagerComponent with
    KafkaEventStoreComponent {

  val clock = Clock.System

  def usageLoggingFactory(config: Configuration) = new NullUsageLogging("")

  implicit val asyncContext = defaultFutureDispatch
}



trait KafkaEventStoreComponent extends AkkaDefaults with Logging {

  def eventStoreFactory(config: Configuration): EventStore = {

    val centralZookeeperHosts = getConfig(config, "central.zk.connect")

    val serviceUID = ZookeeperSystemCoordination.extractServiceUID(config)
    val coordination = ZookeeperSystemCoordination(centralZookeeperHosts, serviceUID, true)
    val agent = serviceUID.hostId + serviceUID.serviceId  

    val eventIdSeq = new SystemEventIdSequence(agent, coordination)

    val localConfig = config.detach("local")
    val centralConfig = config.detach("central")

    val eventStore = new LocalKafkaEventStore(localConfig)
    val relayAgent = new KafkaRelayAgent(eventIdSeq, localConfig, centralConfig)

    new EventStore {
      def save(event: Event, timeout: Timeout) = eventStore.save(event, timeout)
      def start() = relayAgent.start flatMap { _ => eventStore.start }
      def stop() = eventStore.stop flatMap { _ => relayAgent.stop }
    }
  }

  def getConfig(cfg: Configuration, key: String): String = cfg.get[String](key).getOrElse(
    sys.error("Invalid configuration eventStore.%s required".format(key))
  )

}

