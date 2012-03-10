package com.precog.ingest
package util

import kafka._

import com.precog.common._
import com.precog.common.security._
import com.precog.common.util._

import java.util.Properties
import java.io.{File, FileReader}

import _root_.kafka._
import _root_.kafka.api._
import _root_.kafka.consumer._
import _root_.kafka.producer._

object DirectKafkaConsumer extends App {
  val config = new Properties()
  config.put("groupid", "test_group_2")
  config.put("enable.zookeeper", "false")
  config.put("broker.list", "0:localhost:9092")
  config.put("autocommit.enable", "false")
  
  val topic = "direct_test_topic"

  val simpleConsumer = new SimpleConsumer("localhost", 9092, 5000, 64 * 1024)

  val codec = new KafkaIngestMessageCodec

  var offset: Long = 0
  var batch = 0
  var msgs: Long = 0
  val start = System.nanoTime
  while (true) { 
    // create a fetch request for topic “test”, partition 0, current offset, and fetch size of 1MB
    val fetchRequest = new FetchRequest(topic, 0, offset, 1000000)

    // get the message set from the consumer and print them out
    val messages = simpleConsumer.fetch(fetchRequest)
    messages foreach { msg =>
      // advance the offset after consuming each message
      offset = msg.offset
      msgs += 1
      if(msgs % 1000 == 0) {
        System.out.println("consumed: " + codec.toEvent(msg.message));
        val now = System.nanoTime
        val secs = (now-start)/1000000000.0
        val throughput = msgs / secs
        println("Message %d batch %d time %.02fs throughput %.01f msgs/s".format(msgs, batch, secs, throughput))
      }
    }

    batch += 1
  } 
}

object DirectKafkaProducer extends App {
  val config = new Properties()
  config.put("broker.list", "0:localhost:9092")
  config.put("enable.zookeeper", "false")
  config.put("serializer.class", "com.precog.ingest.kafka.KafkaIngestMessageCodec")
 
  val producer = new Producer[String, IngestMessage](new ProducerConfig(config))

  val topic = "direct_test_topic"
 
  val sample = DistributedSampleSet(0, sampler = AdSamples.adCampaignSample)
  val event = Event.fromJValue(Path("/test/"), sample.next._1, StaticTokenManager.rootUID)
  val msg = EventMessage(0,0,event) 

  val total = 1000000
  val start = System.nanoTime
  for(i <- 0 to total) {
    if(i % 1000 == 0) {
      val now = System.nanoTime
      val secs = (now-start)/1000000000.0
      val throughput = i / secs
      println("Message %d time %.02fs throughput %.01f msgs/s".format(i, secs, throughput))
    }
    val data = new ProducerData[String, IngestMessage](topic, msg)
    producer.send(data)
  }

  producer.close
}
