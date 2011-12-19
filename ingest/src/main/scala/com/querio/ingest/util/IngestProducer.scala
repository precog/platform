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
