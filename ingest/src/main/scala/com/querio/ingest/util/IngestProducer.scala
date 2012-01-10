package com.querio.ingest.util

import scala.collection.mutable.ListBuffer

import java.util.Properties
import java.io.{File, FileReader}

import com.reportgrid.common._
import com.querio.ingest.api._
import com.querio.ingest.service._

import blueeyes.concurrent.Future

import blueeyes.json.JsonAST._

import blueeyes.core.http.MimeTypes._
import blueeyes.core.data.BijectionsChunkJson._
import blueeyes.core.http.HttpResponse
import blueeyes.core.service.HttpClient
import blueeyes.core.service.engines.HttpClientXLightWeb

import scalaz.NonEmptyList

abstract class IngestProducer(args: Array[String]) extends RealisticIngestMessage {

  lazy val config = loadConfig(args)

  lazy val messages = config.getProperty("messages", "1000").toInt
  lazy val delay = config.getProperty("delay", "100").toInt
  lazy val threadCount = config.getProperty("threads", "1").toInt

  def run() {

    val start = System.nanoTime

    val threads = 0.until(threadCount).map(_ => new Thread() {
      override def run() {
        0.until(messages).foreach { i =>
          if(i % 10 == 0) println("Sending: " + i)
          send(genEvent.sample.get)
          if(delay > 0) {
            Thread.sleep(delay)
        }
      }
    }})

    threads.foreach(_.start)
    threads.foreach(_.join)

    close

    println("Time: %.02f".format((System.nanoTime - start) / 1000000000.0))
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
messages - number of messages to produce (default: 1000)
delay - delay between messages (<0 indicates no delay) (default: 100)
threads - number of producer threads (default: 1)
    """
  
  def send(event: Event): Unit
  def close(): Unit = ()
}

object WebappIngestProducer {
  def main(args: Array[String]) =  new WebappIngestProducer(args).run()
}

class WebappIngestProducer(args: Array[String]) extends IngestProducer(args) {


  lazy val base = config.getProperty("serviceUrl", "http://localhost:30050/vfs/")
  val client = new HttpClientXLightWeb 

  def send(event: Event) {

    val tokens = Event.extractOwners(event)

    val f: Future[HttpResponse[JValue]] = client.path(base)
                                                .query("tokenId", tokens.head)
                                                .contentType(application/json)
                                                .post[JValue](event.path)(Event.dataRepresentation(event.content))
    while(!f.isDone) {}
    if(f.isCanceled) {
      println("Error tracking data: " + f.error)
    }
  }

  override def usageMessage = super.usageMessage + """
serviceUrl - base url for web application (default: http://localhost:30050/vfs/)
  """
}

object DirectIngestProducer {
  def main(args: Array[String]) = new DirectIngestProducer(args).run()
}

class DirectIngestProducer(args: Array[String]) extends IngestProducer(args) {

  lazy val testTopic = config.getProperty("topicId", "test-topic-1")
  lazy val zookeeperHosts = config.getProperty("zookeeperHosts", "127.0.0.1:2181")
  lazy val store = kafkaStore(testTopic)

  def send(event: Event) {
    store.save(genEvent.sample.get)
  }

  def kafkaStore(topic: String): EventStore = {
    val props = new Properties()
    props.put("zk.connect", zookeeperHosts) 
    props.put("serializer.class", "com.querio.ingest.api.IngestMessageCodec")
  
    val defaultAddresses = NonEmptyList(MailboxAddress(0))

    val routeTable = new ConstantRouteTable(defaultAddresses)
    val messaging = new SimpleKafkaMessaging(topic, props)

    val qz = QuerioZookeeper.testQuerioZookeeper(zookeeperHosts)
    qz.setup
    val producerId = qz.acquireProducerId
    qz.close

    new EventStore(new EventRouter(routeTable, messaging), producerId)
  }
  
  override def usageMessage = super.usageMessage + """
topicId - kafka topic to publish events to (default: test-topic-1 )
zookeeperHosts - comma delimeted list of zookeeper hosts (default: 127.0.0.1:2181)
  """

  override def close() {
    store.close.await
  }
}
