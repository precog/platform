package com.precog.ingest
package util

import kafka._ 

import scala.collection.mutable.ListBuffer

import java.util.Properties
import java.io.{File, FileReader}

import com.precog.analytics.{Path, Token}
import com.precog.common._
import com.precog.common.util.RealisticIngestMessage
import com.precog.common.util.AdSamples
import com.precog.common.util.DistributedSampleSet
import com.precog.ingest.service._

import akka.actor.ActorSystem
import akka.dispatch.Future
import akka.dispatch.Await
import akka.util.duration._

import blueeyes.bkka.AkkaDefaults

import blueeyes.core.http.MimeTypes
import blueeyes.core.http.MimeTypes._
import blueeyes.core.data.BijectionsChunkJson._
import blueeyes.core.http.HttpResponse
import blueeyes.core.http.HttpStatusCodes.OK
import blueeyes.core.service.HttpClient
import blueeyes.core.service.engines.HttpClientXLightWeb

import blueeyes.json.JsonAST._

import scalaz.NonEmptyList

abstract class IngestProducer(args: Array[String]) extends RealisticIngestMessage {

  lazy val config = loadConfig(args)

  lazy val messages = config.getProperty("messages", "1000").toInt
  lazy val delay = config.getProperty("delay", "100").toInt
  lazy val threadCount = config.getProperty("threads", "1").toInt
  lazy val rawRepeats = config.getProperty("repeats", "1").toInt
  lazy val repeats = if(rawRepeats < 1) Int.MaxValue-2 else rawRepeats
  lazy val verbose = config.getProperty("verbose", "true").toBoolean

  def run() {
    for(r <- 0 to repeats) {
      val start = System.nanoTime

      val samples = List(
        ("/campaigns/", DistributedSampleSet(0, sampler = AdSamples.adCampaignSample _)),
        ("/organizations/", DistributedSampleSet(0, sampler = AdSamples.adOrganizationSample _)),
        ("/clicks/", DistributedSampleSet(0, sampler = AdSamples.interactionSample _)),
        ("/impressions/", DistributedSampleSet(0, sampler = AdSamples.interactionSample _)),
        ("/impressions2/", DistributedSampleSet(0, sampler = AdSamples.interactionSampleISO8601 _)))

      val testRuns = 0.until(threadCount).map(_ => new TestRun(samples))

      val threads = testRuns.map(testRun => new Thread(testRun))

      threads.foreach(_.start)
      threads.foreach(_.join)

      val totalErrors = testRuns map { _.errorCount } reduce { _ + _ }

      val seconds = (System.nanoTime - start) / 1000000000.0

      val totalMessages = messages * threadCount * samples.size

      println("Time: %.02f Messages: %d Throughput: %.01f msgs/s Errors: %d".format(seconds, totalMessages, totalMessages / seconds, totalErrors))
    } 
    close
  }

  class TestRun(samples: List[(String, DistributedSampleSet)]) extends Runnable {
    private var errors = 0
    def errorCount = errors
      override def run() {
        samples.foreach {
          case (path, sample) =>
            def event = Event.fromJValue(Path(path), sample.next._1, Token.Root.tokenId)
            0.until(messages).foreach { i =>
              if(i % 10 == 0 && verbose) println("Sending to [%s]: %d".format(path, i))
              try {
                send(event)
              } catch {
                case ex => 
                  ex.printStackTrace
                  errors += 1
              }
              if(delay > 0) {
                Thread.sleep(delay)
            }
        }
      }
    }
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
repeats - number of of times to repeat test (default: 1)
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

    val f: Future[HttpResponse[JValue]] = client.path(base)
                                                .query("tokenId", event.tokenId)
                                                .contentType(application/MimeTypes.json)
                                                .post[JValue](event.path.toString)(event.data)
    Await.ready(f, 10 seconds) 
    f.value match {
      case Some(Right(HttpResponse(status, _, _, _))) if status.code == OK => ()
      case Some(Right(HttpResponse(status, _, _, _)))                       => 
        throw new RuntimeException("Server returned error code with request")
      case Some(Left(ex))                                              => 
        throw ex
      case _                                                           => 
        throw new RuntimeException("Error processing insert request") 
    }
  }

  override def usageMessage = super.usageMessage + """
serviceUrl - base url for web application (default: http://localhost:30050/vfs/)
  """

  override def close(): Unit = AkkaDefaults.actorSystem.shutdown
}

object DirectIngestProducer {
  def main(args: Array[String]) = new DirectIngestProducer(args).run()
}

class DirectIngestProducer(args: Array[String]) extends IngestProducer(args) {
  implicit val actorSystem = ActorSystem("direct_ingest")

  lazy val testTopic = config.getProperty("topicId", "test-topic-1")
  lazy val zookeeperHosts = config.getProperty("zookeeperHosts", "127.0.0.1:2181")
  lazy val store = kafkaStore(testTopic)

  def send(event: Event) {
    store.save(event)
  }

  def kafkaStore(topic: String): KafkaEventStore = {
    val props = new Properties()
    props.put("zk.connect", zookeeperHosts) 
    props.put("serializer.class", "com.precog.ingest.api.IngestMessageCodec")
  
    val defaultAddresses = NonEmptyList(MailboxAddress(0))

    val routeTable = new ConstantRouteTable(defaultAddresses)
    val messaging = new KafkaMessaging(topic, props)

    val producerId = 0

    new KafkaEventStore(new EventRouter(routeTable, messaging), producerId)
  }
  
  override def usageMessage = super.usageMessage + """
topicId - kafka topic to publish events to (default: test-topic-1 )
zookeeperHosts - comma delimeted list of zookeeper hosts (default: 127.0.0.1:2181)
  """

  override def close() {
    Await.result(store.stop, 10 seconds)
    actorSystem.shutdown
  }
}
