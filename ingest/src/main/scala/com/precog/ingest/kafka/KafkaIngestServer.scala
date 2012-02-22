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

import akka.dispatch.MessageDispatcher

import com.precog.common._
import com.precog.common.util.ZookeeperSystemCoordination

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import java.net.InetAddress

import net.lag.configgy.ConfigMap
import scalaz.NonEmptyList

trait KafkaEventStoreComponent {

  implicit def defaultFutureDispatch: MessageDispatcher

  def eventStoreFactory(eventConfig: ConfigMap): EventStore = {
    val localTopic = getConfig(eventConfig, "local.topic")
    val localBroker = getConfig(eventConfig, "local.broker")
    
    val centralTopic = getConfig(eventConfig, "central.topic")
    val centralZookeeperHosts = getConfig(eventConfig, "central.zookeeperHosts")

    val localConfig = new Properties()
    localConfig.put("broker.list", localBroker)
    localConfig.put("serializer.class", "com.precog.ingest.kafka.KafkaEventCodec")
    
    val centralConfig = new Properties()
    centralConfig.put("zk.connect", centralZookeeperHosts)
    centralConfig.put("serializer.class", "com.precog.ingest.kafka.KafkaIngestMessageCodec")

    val coordination = ZookeeperSystemCoordination.testZookeeperSystemCoordination(centralZookeeperHosts)

    val agent = InetAddress.getLocalHost.getHostName

    val eventIdSeq = new SystemEventIdSequence(agent, coordination)

    val eventStore = new LocalKafkaEventStore(localTopic, localConfig)
    val relayAgent = new KafkaRelayAgent(eventIdSeq, localTopic, localConfig, centralTopic, centralConfig)

    new EventStore {
      def save(event: Event) = eventStore.save(event)
      def start() = relayAgent.start flatMap { _ => eventStore.start }
      def stop() = eventStore.stop flatMap { _ => relayAgent.stop }
    }
  }

  def getConfig(cfg: ConfigMap, key: String): String = cfg.getString(key).getOrElse(
    sys.error("Invalid configuration eventStore.%s required".format(key))
  )

}

object KafkaIngestServer extends IngestServer with KafkaEventStoreComponent
