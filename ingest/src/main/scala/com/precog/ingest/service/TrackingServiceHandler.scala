package com.precog
package ingest
package service

import com.precog.ingest.util._
import common._
import common.security._

import akka.dispatch.{ Future, Promise }
import akka.dispatch.MessageDispatcher
import akka.util.Timeout

import blueeyes.core.data.ByteChunk
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._

import blueeyes.json.JsonParser
import blueeyes.json.JsonParser.ParseException
import blueeyes.json.JsonAST._
import blueeyes.json.JPath

import java.util.concurrent.{ Executor, RejectedExecutionException }

import java.io.{ File, FileReader, BufferedReader, FileInputStream, FileOutputStream, Closeable }
import java.nio.channels._
import java.nio.ByteBuffer

import com.weiglewilczek.slf4s.Logging

import au.com.bytecode.opencsv.CSVReader

import scala.collection.mutable.ListBuffer

import scalaz._

class TrackingServiceHandler(accessControl: AccessControl[Future], eventStore: EventStore, usageLogging: UsageLogging, insertTimeout: Timeout, threadPool: Executor, maxBatchErrors: Int)(implicit dispatcher: MessageDispatcher)
extends CustomHttpService[Either[Future[JValue], ByteChunk], (Token, Path) => Future[HttpResponse[JValue]]] with Logging {

  def writeChunkStream(chan: WritableByteChannel, chunk: ByteChunk): Future[Unit] = {
    Future { chan.write(ByteBuffer.wrap(chunk.data)) } flatMap { _ =>
      chunk.next match {
        case Some(future) => future flatMap (writeChunkStream(chan, _))
        case None => Future(chan.close())
      }
    }
  }

  def ingest(p: Path, t: Token, event: JValue): Future[Unit] = {
    
    val eventInstance = Event.fromJValue(p, event, t.tid)
    logger.trace("Saving event: " + eventInstance)
    eventStore.save(eventInstance, insertTimeout)
  }

  case class SyncResult(total: Int, ingested: Int, errors: List[(Int, String)])

  class EventQueueInserter(p: Path, t: Token, events: Iterator[Either[String, JValue]], close: Option[Closeable]) extends Runnable {
    private[service] val result: Promise[SyncResult] = Promise()

    def run() {
      val errors: ListBuffer[(Int, String)] = new ListBuffer()
      val futures: ListBuffer[Future[Unit]] = new ListBuffer()
      var i = 0
      while (events.hasNext) {
        val ev = events.next()
        if (errors.size < maxBatchErrors) {
          ev match {
            case Right(event) => futures += ingest(p, t, event)
            case Left(error) => errors += (i -> error)
          }
        }
        i += 1
      }

      Future.sequence(futures) foreach { results =>
        close foreach (_.close())
        result.complete(Right(SyncResult(i, futures.size, errors.toList)))
      }
    }
  }

  def writeToFile(byteStream: ByteChunk): Future[File] = {
    val file = File.createTempFile("async-ingest-", null)
    val outChannel = new FileOutputStream(file).getChannel()
    for {
      _ <- writeChunkStream(outChannel, byteStream)
    } yield file
  }

  private def toRows(csv: CSVReader): Iterator[Array[String]] = new Iterator[Array[String]] {
    var nextRow = csv.readNext()
    def hasNext = nextRow != null
    def next() = {
      val n = nextRow
      nextRow = csv.readNext()
      n
    }
  }

  private def csvReaderFor(request: HttpRequest[_]): ValidationNEL[String, File => CSVReader] = {
    import scalaz.syntax.applicative._
    import scalaz.Validation._

    def charOrError(s: Option[String], default: Char): Validation[String, Char] = s map {
        case s if s.length == 1 => success(s.charAt(0))
        case _ => failure("Expected a single character but found a string.")
      } getOrElse success(default)

    val delimiter = charOrError(request.parameters get 'delimiter, ',').toValidationNEL
    val quote = charOrError(request.parameters get 'quote, '"').toValidationNEL
    val escape = charOrError(request.parameters get 'escape,'\\').toValidationNEL

    (delimiter |@| quote |@| escape) { (delimiter, quote, escape) => 
      (file: File) => {
        val reader = new FileReader(file)
        new CSVReader(reader, delimiter, quote, escape)
      }
    }
  }

  def parseCsv(byteStream: ByteChunk, t: Token, p: Path, readCsv: File => CSVReader): Future[EventQueueInserter] = {
    for {
      file <- writeToFile(byteStream)
    } yield {
      val csv0 = readCsv(file)
      val types = CsvType.inferTypes(toRows(csv0))
      csv0.close()

      val csv = readCsv(file)
      val rows = toRows(csv)
      val paths = rows.next() map (JPath(_))
      val jVals: Iterator[Either[String, JValue]] = rows map { row =>
        Right((paths zip types zip row).foldLeft(JNothing: JValue) { case (obj, ((path, tpe), s)) =>
          JValue.unsafeInsert(obj, path, tpe(s))
        })
      }

      new EventQueueInserter(p, t, jVals, Some(csv))
    }
  }

  def parseJson(channel: ReadableByteChannel, t: Token, p: Path): EventQueueInserter = {
    val reader = new BufferedReader(Channels.newReader(channel, "UTF-8"))
    val lines = Iterator.continually(reader.readLine()).takeWhile(_ != null)
    val inserter = new EventQueueInserter(p, t, lines map { json =>
      try {
        Right(JsonParser.parse(json))
      } catch {
        case e: ParseException => Left("Parsing failed: " + e.getMessage())
        case e => Left("Server error: " + e.getMessage())
      }
    }, Some(reader))
    inserter
  }

  def parseSyncJson(byteStream: ByteChunk, t: Token, p: Path): Future[EventQueueInserter] = {
    val pipe = Pipe.open()
    writeChunkStream(pipe.sink(), byteStream)
    Future { parseJson(pipe.source(), t, p) }
  }

  def parseAsyncJson(byteStream: ByteChunk, t: Token, p: Path): Future[EventQueueInserter] = {
    for {
      file <- writeToFile(byteStream)
    } yield {
      parseJson(new FileInputStream(file).getChannel(), t, p)
    }
  }

  def execute(inserter: EventQueueInserter, async: Boolean): Future[HttpResponse[JValue]] = try {
    
    threadPool.execute(inserter)
    if (async) {
      Future { HttpResponse[JValue](Accepted) }
    } else {
      inserter.result map { case SyncResult(total, ingested, errors) =>
        val failed = errors.size
        HttpResponse[JValue](OK, content = Some(JObject(List(
          JField("total", JNum(total)),
          JField("ingested", JNum(ingested)),
          JField("failed", JNum(failed)),
          JField("skipped", JNum(total - ingested - failed)),
          JField("errors", JArray(errors map { case (line, msg) =>
            JObject(List(JField("line", JNum(line)), JField("reason", JString(msg))))
          }))))))
      }
    }
  } catch {
    case _: RejectedExecutionException => Future(HttpResponse[JValue](ServiceUnavailable))
  }

  val service = (request: HttpRequest[Either[Future[JValue], ByteChunk]]) => {
    Success { (t: Token, p: Path) =>
      accessControl.mayAccess(t.tid, p, Set(), WritePermission) flatMap {
        case true => try {
          request.content map {
            case Left(futureEvent) =>
              for {
                event <- futureEvent
                _ <- ingest(p, t, event)
              } yield HttpResponse[JValue](OK)

            case Right(byteStream) =>
              import MimeTypes._
              import Validation._

              val async = request.parameters.get('sync) map (_ == "async") getOrElse false
              val parser = if (request.mimeTypes contains (text / csv)) {
                csvReaderFor(request) map (parseCsv(byteStream, t, p, _))
              } else if (async) {
                success(parseAsyncJson(byteStream, t, p))
              } else  {
                success(parseSyncJson(byteStream, t, p))
              }

              parser match {
                case Success(inserter) =>
                  inserter flatMap (execute(_, async))
                case Failure(errors) => Future {
                  HttpResponse[JValue](BadRequest, content=Some(JArray(errors.list map (JString(_)))))
                }
              }

          } getOrElse {
            Future(HttpResponse[JValue](BadRequest, content=Some(JString("Missing event data."))))
          }
        } catch {
          case _ => Future(HttpResponse[JValue](ServiceUnavailable))
        }

        case false =>
          Future(HttpResponse[JValue](Unauthorized, content=Some(JString("Your token does not have permissions to write at this location."))))
      }
    }
  }

  val metadata = Some(DescriptionMetadata(
    """
      This service can be used to store an data point with or without an associated timestamp. 
      Timestamps are not added by default.
    """
  ))
}
