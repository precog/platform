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
