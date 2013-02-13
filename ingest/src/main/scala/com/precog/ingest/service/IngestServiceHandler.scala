package com.precog
package ingest
package service

import com.precog.ingest.util._
import accounts._
import common._
import common.security._
import com.precog.util.PrecogUnit

import blueeyes.bkka._
import akka.dispatch.{ExecutionContext, Future, Promise}
import akka.dispatch.MessageDispatcher
import akka.util.Timeout

import blueeyes.core.data.ByteChunk
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._

import blueeyes.json._

import com.google.common.base.Charsets

import java.util.concurrent.{ Executor, RejectedExecutionException }

import java.io.{ File, FileReader, BufferedReader, FileInputStream, FileOutputStream, Closeable }
import java.nio.channels._
import java.nio.ByteBuffer

import com.weiglewilczek.slf4s.Logging

import au.com.bytecode.opencsv.CSVReader

import scala.collection.mutable.ListBuffer

import scalaz._
import scalaz.effect.IO

class IngestServiceHandler(accessControl: AccessControl[Future], eventStore: EventStore, insertTimeout: Timeout, threadPool: Executor, maxBatchErrors: Int)(implicit dispatcher: MessageDispatcher)
extends CustomHttpService[Either[Future[JValue], ByteChunk], (APIKeyRecord, Path) => Future[HttpResponse[JValue]]] with Logging {

  protected implicit val M = new FutureMonad(ExecutionContext.fromExecutor(threadPool))

  def ensureByteBufferSanity(bb: ByteBuffer) = {
    if (bb.remaining == 0) {
      // Oh hell no. We don't need no stinkin' pre-read ByteBuffers
      bb.rewind()
    }
  }

  def writeChunkStream(chan: WritableByteChannel, chunk: ByteChunk): Future[PrecogUnit] = {
    logger.trace("Writing %s to %s".format(chunk, chan))
    def writeChannel(stream: StreamT[Future, ByteBuffer]): Future[PrecogUnit] = {
      stream.uncons flatMap {
        case Some((bb, tail)) => { ensureByteBufferSanity(bb); val written = chan.write(bb); logger.trace("Wrote %d bytes".format(written)); writeChannel(tail); }
        case None => Future { chan.close(); PrecogUnit }
      }
    }

    chunk match {
      case Left(bb) => Future { ensureByteBufferSanity(bb); val written = chan.write(bb); logger.trace("Wrote %d bytes".format(written)); chan.close(); PrecogUnit }
      case Right(stream) => writeChannel(stream)
    }
  }

  def ingest(r: APIKeyRecord, p: Path, event: JValue): Future[PrecogUnit] = {
    val eventInstance = Event.fromJValue(r.apiKey, p, None, event)
    eventStore.save(eventInstance, insertTimeout)
  }

  case class SyncResult(total: Int, ingested: Int, errors: List[(Int, String)])

  class EventQueueInserter(t: APIKeyRecord, p: Path, events: Iterator[Either[String, JValue]], close: IO[Unit]) extends Runnable {
    private[service] val result: Promise[SyncResult] = Promise()

    def run() {
      val errors: ListBuffer[(Int, String)] = new ListBuffer()
      val futures: ListBuffer[Future[PrecogUnit]] = new ListBuffer()
      var i = 0
      while (events.hasNext) {
        val ev = events.next()
        if (errors.size < maxBatchErrors) {
          ev match {
            case Right(event) if event.flattenWithPath.size <= 250 => futures += ingest(t, p, event)
            case Right(event) => 
              logger.warn("Event with more than 250 fields skipped: " + event)
              errors += (i -> "Cannot ingest values with more than 250 primitive fields. This limitiation will be lifted in a future release. Thank you for your patience.")
            case Left(error) => errors += (i -> error)
          }
        }
        i += 1
      }

      logger.debug("Insert started on %d events".format(i))

      Future.sequence(futures) foreach { results =>
        close.unsafePerformIO
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

  def parseCsv(byteStream: ByteChunk, t: APIKeyRecord, p: Path, readCsv: File => CSVReader): Future[EventQueueInserter] = {
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
        Right((paths zip types zip row).foldLeft(JUndefined: JValue) { case (obj, ((path, tpe), s)) =>
          JValue.unsafeInsert(obj, path, tpe(s))
        })
      }

      new EventQueueInserter(t, p, jVals, IO { csv.close; file.delete })
    }
  }

  def parseJson(channel: ReadableByteChannel, t: APIKeyRecord, p: Path, cleanup: IO[Unit]): EventQueueInserter = {
    val reader = new BufferedReader(Channels.newReader(channel, Charsets.UTF_8.name))
    val lines = Iterator.continually { reader.readLine() }.takeWhile(_ != null)
    new EventQueueInserter(t, p, lines map { json => JParser.parseFromString(json).bimap("Parsing failed: " + _.getMessage, x => x).toEither }, IO(reader.close).flatMap {_ => cleanup })
  }

  def parseSyncJson(byteStream: ByteChunk, t: APIKeyRecord, p: Path): Future[EventQueueInserter] = {
    val pipe = Pipe.open()
    writeChunkStream(pipe.sink(), byteStream)
    Future(parseJson(pipe.source(), t, p, IO.ioUnit))
  }

  def parseAsyncJson(byteStream: ByteChunk, t: APIKeyRecord, p: Path): Future[EventQueueInserter] = {
    for {
      file <- writeToFile(byteStream)
    } yield {
      parseJson(new FileInputStream(file).getChannel(), t, p, IO { file.delete })
    }
  }

  def execute(inserter: EventQueueInserter, async: Boolean): Future[HttpResponse[JValue]] = try {
    threadPool.execute(inserter)
    if (async) {
      Promise.successful(HttpResponse[JValue](Accepted))
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
    Success { (r: APIKeyRecord, p: Path) =>
      accessControl.hasCapability(r.apiKey, Set(WritePermission(p, Set())), None) flatMap {
        case true => try {
          logger.debug("Ingesting events as: APIKey: "+r.apiKey+" path: "+p)
          request.content map {
            case Left(futureEvent) =>
              for {
                event <- futureEvent
                _ <- ingest(r, p, event)
              } yield HttpResponse[JValue](OK)

            case Right(byteStream) =>
              import MimeTypes._
              import Validation._

              val async = request.parameters.get('sync) map (_ == "async") getOrElse false
              val parser = if (request.mimeTypes contains (text / csv)) {
                csvReaderFor(request) map (parseCsv(byteStream, r, p, _))
              } else if (async) {
                success(parseAsyncJson(byteStream, r, p))
              } else  {
                success(parseSyncJson(byteStream, r, p))
              }

              parser match {
                case Success(inserter) =>
                  inserter flatMap { execute(_, async) }
                case Failure(errors) => 
                  logger.debug("Errors during ingest: " + errors.list.mkString("  ", "\n  ", ""))
                  Promise.successful(
                    HttpResponse[JValue](BadRequest, content=Some(JArray(errors.list map (JString(_)))))
                  )
              }

          } getOrElse {
            Future(HttpResponse[JValue](BadRequest, content=Some(JString("Missing event data."))))
          }
        } catch {
          case _ => Future(HttpResponse[JValue](ServiceUnavailable))
        }

        case false =>
          Future(HttpResponse[JValue](Unauthorized, content=Some(JString("Your API key does not have permissions to write at this location."))))
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
