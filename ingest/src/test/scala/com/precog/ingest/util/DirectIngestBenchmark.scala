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

import kafka._

import com.precog.common._
import com.precog.common.kafka._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.common.util._
import com.precog.ingest.service._

import java.util.Properties
import java.io.{File, FileReader}

import org.joda.time.Instant

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
        System.out.println("consumed: " + EventMessageEncoding.read(msg.message.buffer));
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
  val msg = IngestMessage("test", Path("/test/"), Authorities("test"), Vector(IngestRecord(EventId(0, 0), sample.next._1)), None, new Instant(), StreamRef.Append)

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

// type DirectIngestBenchmark
