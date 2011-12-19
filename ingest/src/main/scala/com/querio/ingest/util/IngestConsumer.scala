package com.querio.ingest.util

import java.util.Properties

import com.querio.ingest.api._
import com.querio.ingest.service._

import kafka.consumer._

object IngestConsumer {
  
  val topic = "test-topic-0"

  def main(args: Array[String]) {
   
    val stream = kafkaConsumer(topic)
 
    println("waiting on stream")

    stream.foreach(println)
  }

  def kafkaConsumer(topic: String): KafkaMessageStream[IngestMessage] = {
    val props = new Properties()
    props.put("zk.connect", "127.0.0.1:2181")
    props.put("zk.connectiontimeout.ms", "1000000")
    props.put("groupid", "test_group")
   
    val config = new ConsumerConfig(props) 
    val connector = Consumer.create(config) 

    val streams = connector.createMessageStreams[IngestMessage](Map(topic -> 1), new IngestMessageCodec)
    streams(topic)(0)
  }
}
