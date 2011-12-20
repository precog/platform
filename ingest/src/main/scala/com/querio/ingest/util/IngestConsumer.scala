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

import kafka.consumer._

object IngestConsumer {
  val topic = "test-topic-0"

  def main(args: Array[String]) {
    val rec = kafkaReceiver(topic)

    while(rec.hasNext) {
      println(rec.next)
    }
  }

  def kafkaReceiver(topic: String) = { 
    val config = new Properties()
    config.put("zk.connect", "127.0.0.1:2181")
    config.put("zk.connectiontimeout.ms", "1000000")
    config.put("groupid", "test_group")
  
    new KafkaIngestMessageReceiver(topic, config)
  }
}
