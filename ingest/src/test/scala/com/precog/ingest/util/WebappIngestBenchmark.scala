package com.precog.ingest
package util

import kafka._
import service._

import scala.collection.mutable.ListBuffer

import java.util.Properties
import java.io.{File, FileReader}

import com.precog.common._
import com.precog.util.IOUtils
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.common.util._

import akka.actor.ActorSystem
import akka.dispatch.Future
import akka.dispatch.Await
import akka.util.Timeout
import akka.util.duration._

import blueeyes.bkka.AkkaDefaults

import blueeyes.core.http.MimeTypes
import blueeyes.core.http.MimeTypes._
import blueeyes.core.data.ByteChunk
import blueeyes.core.data.DefaultBijections._
import blueeyes.core.http.HttpRequest
import blueeyes.core.http.HttpResponse
import blueeyes.core.http.HttpStatusCodes.OK
import blueeyes.core.service._
import blueeyes.core.service.HttpClient
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.core.service.AsyncHttpTranscoder

import blueeyes.json._

import org.joda.time.Instant

import scalaz.NonEmptyList

abstract class IngestProducer(args: Array[String]) extends RealisticEventMessage with AkkaDefaults {

  lazy val config = loadConfig(args)

  lazy val messages = config.getProperty("messages", "1000").toInt
  lazy val delay = config.getProperty("delay", "100").toInt
  lazy val threadCount = config.getProperty("threads", "1").toInt
  lazy val rawRepeats = config.getProperty("repeats", "1").toInt
  lazy val repeats = if(rawRepeats < 1) Int.MaxValue-2 else rawRepeats
  lazy val verbose = config.getProperty("verbose", "true").toBoolean

  def run() {
    for(r <- 0 until repeats) {
      val start = System.nanoTime

      val samples = List(
        ("/campaigns/", DistributedSampleSet(0, sampler = AdSamples.adCampaignSample)),
        ("/organizations/", DistributedSampleSet(0, sampler = AdSamples.adOrganizationSample)),
        ("/impressions/", DistributedSampleSet(0, sampler = AdSamples.interactionSample)),
        ("/clicks/", DistributedSampleSet(0, sampler = AdSamples.interactionSample2)),
        ("/events/", DistributedSampleSet(0, sampler = AdSamples.eventsSample)))

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

  class TestRun(samples: List[(String, DistributedSampleSet[JObject])]) extends Runnable {
    private var errors = 0
    val timeout = new Timeout(120000)
    def errorCount = errors
      override def run() {
        samples.foreach {
          case (path, sample) =>
            val event = Ingest("bogus", Path(path), None, Vector(sample.next._1), None, new Instant(), StreamRef.Append)

            0.until(messages).foreach { i =>
              if(i % 10 == 0 && verbose) println("Sending to [%s]: %d".format(path, i))
              try {
                send(event, timeout)
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

  def send(event: Ingest, timeout: Timeout): Unit
  def close(): Unit = ()
}

object JsonLoader extends App with AkkaDefaults {
  implicit val M: scalaz.Monad[Future] = new blueeyes.bkka.FutureMonad(defaultFutureDispatch)

  def usage() {
    println(
"""
Usage:

  command {host} {API key} {json data file}
"""
    )
  }

  val client = new HttpClientXLightWeb

  def run(url: String, apiKey: String, datafile: String) {
    val data = IOUtils.readFileToString(new File(datafile)).unsafePerformIO
    val json = JParser.parseUnsafe(data)
    json match {
      case JArray(elements) => elements.foreach { send(url, apiKey, _ ) }
      case _                =>
        println("Error the input file must contain an array of elements to insert")
        System.exit(1)
    }
  }

  def send(url: String, apiKey: String, event: JValue) {

    val f: Future[HttpResponse[JValue]] = client.path(url)
                                                .query("apiKey", apiKey)
                                                .contentType(application/MimeTypes.json)
                                                .post[JValue]("")(event)
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

  if(args.size < 3) {
    usage()
    System.exit(1)
  } else {
    run(args(0), args(1), args(2))
  }

  AkkaDefaults.actorSystem.shutdown
}

object WebappIngestProducer {
  def main(args: Array[String]) =  new WebappIngestProducer(args).run()
}

class WebappIngestProducer(args: Array[String]) extends IngestProducer(args) {

  lazy val base = config.getProperty("serviceUrl", "http://localhost:30050/vfs/")
  lazy val ingestAPIKey = config.getProperty("apiKey", "dummy")
  val ingestOwnerAccountId = Authorities("dummy")
  val client = new HttpClientXLightWeb

  implicit val M: scalaz.Monad[Future] = new blueeyes.bkka.FutureMonad(defaultFutureDispatch)

  def send(event: Ingest, timeout: Timeout) {
    // FIXME: expects ingest to be of a single value only.
    val f: Future[HttpResponse[JValue]] = client.path(base)
                                                .query("apiKey", ingestAPIKey)
                                                .contentType(application/MimeTypes.json)
                                                .post[JValue](event.path.toString)(event.data.head)
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

// type WebappIngestBenchmark
