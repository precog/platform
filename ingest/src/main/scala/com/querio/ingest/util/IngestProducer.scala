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
package com.querio.ingest.util

import java.util.Properties

import com.querio.ingest.api._
import com.querio.ingest.service._

object IngestProducer extends RealisticIngestMessage {
 
  def testTopic = "test-topic-0"
  
  def main(args: Array[String]) {
    val messages = if(args.length == 1) args(0).toInt else 1000
    val delay = if(args.length == 2) args(1).toInt else 100
   
    val store = kafkaStore(testTopic)

    0.until(messages).foreach { i =>
      if(i % 10 == 0) println("Sending: " + i)
      store.save(genEvent.sample.get)
      Thread.sleep(delay)
    }
  }

  def kafkaStore(topic: String): EventStore = {
    val props = new Properties()
    props.put("zk.connect", "127.0.0.1:2181")
    props.put("serializer.class", "com.querio.ingest.api.IngestMessageCodec")
    
    val messageSenderMap = Map() + (MailboxAddress(0L) -> new KafkaMessageSender(topic, props))
    
    val defaultAddresses = List(MailboxAddress(0))
    
    new DefaultEventStore(0,
                          new ConstantEventRouter(defaultAddresses),
                          new MappedMessageSenders(messageSenderMap))
  }
}
