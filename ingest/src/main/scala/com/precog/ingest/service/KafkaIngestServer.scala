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
package com.precog.ingest.service

import akka.dispatch.MessageDispatcher

import com.precog.ingest.api._
import com.precog.ingest.util.QuerioZookeeper

import java.util.Properties
import net.lag.configgy.ConfigMap
import scalaz.NonEmptyList

trait KafkaEventStoreFactory {

  implicit def defaultFutureDispatch: MessageDispatcher

  def eventStoreFactory(eventConfig: ConfigMap): EventStore = {
    val topicId = eventConfig.getString("topicId").getOrElse(sys.error("Invalid configuration eventStore.topicId required"))
    val zookeeperHosts = eventConfig.getString("zookeeperHosts").getOrElse(sys.error("Invalid configuration eventStore.zookeeperHosts required"))
    
    val props = new Properties()
    props.put("zk.connect", zookeeperHosts)
    props.put("serializer.class", "com.precog.ingest.api.IngestMessageCodec")
    
    val defaultAddresses = NonEmptyList(MailboxAddress(0))

    val routeTable = new ConstantRouteTable(defaultAddresses)
    val messaging = new SimpleKafkaMessaging(topicId, props)

    val qz = QuerioZookeeper.testQuerioZookeeper(zookeeperHosts)
    qz.setup
    val producerId = qz.acquireProducerId
    qz.close

    new KafkaEventStore(new EventRouter(routeTable, messaging), producerId)
  }
}

object KafkaIngestServer extends IngestServer with KafkaEventStoreFactory
