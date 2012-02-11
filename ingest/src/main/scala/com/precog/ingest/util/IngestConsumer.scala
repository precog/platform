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
package util

import kafka.KafkaIngestMessageReceiver

import java.util.Properties
import java.io.{File, FileReader}

import _root_.kafka.consumer._

object IngestConsumer {
  def main(args: Array[String]) = new IngestConsumer(args).run 
}

class IngestConsumer(args: Array[String]) {

  lazy val config = loadConfig(args)

  lazy val topic = config.getProperty("topicId", "test-topic-1")
  lazy val zookeeperHosts = config.getProperty("zookeeperHosts", "127.0.0.1:2181")
  lazy val groupId = config.getProperty("consumerGroup", "test_group_1")

  def run() {
    val rec = kafkaReceiver(topic)

    while(rec.hasNext) {
      println(rec.next)
    }
  }

  def kafkaReceiver(topic: String) = { 
    val config = new Properties()
    config.put("zk.connect", zookeeperHosts) 
    config.put("zk.connectiontimeout.ms", "1000000")
    config.put("groupid", groupId)
  
    new KafkaIngestMessageReceiver(topic, config)
  }
  
  def loadConfig(args: Array[String]): Properties = {
    if(args.length != 1) usage() 
    
    val config = new Properties()
    val file = new File(args(0))
    
    if(!file.exists) usage() 
    
    config.load(new FileReader(file))
    config
  }

  def usage() {
    println(usageMessage)
    sys.exit(1)
  }

  def usageMessage = 
    """
Usage: command {properties file}

Properites:
topicId - kafka topic id (default: test-topic-1)
zookeeperHosts - list of zookeeper hosts (default: 127.0.0.1:2181)
consumerGroup - consumerGroup for tracking message consumption (default: test_group_1)
    """
}
