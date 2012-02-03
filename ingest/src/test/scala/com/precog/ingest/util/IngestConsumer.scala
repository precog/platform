package com.precog.ingest.util

import java.util.Properties
import java.io.{File, FileReader}

import com.precog.ingest.api._
import com.precog.ingest.service._

import kafka.consumer._

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
