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
package com.precog
package ingest
package service

import com.precog.ingest.util._
import accounts._
import common._
import common.security._

import blueeyes.bkka._
import akka.dispatch.{ExecutionContext, Future, Promise}
import akka.dispatch.MessageDispatcher
import akka.util.Timeout

import blueeyes.core.data.ByteChunk
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._

import blueeyes.json._

import java.util.concurrent.{ Executor, RejectedExecutionException }

import java.io.{ File, FileReader, BufferedReader, FileInputStream, FileOutputStream, Closeable }
import java.nio.channels._
import java.nio.ByteBuffer

import com.weiglewilczek.slf4s.Logging

import au.com.bytecode.opencsv.CSVReader

import scala.collection.mutable.ListBuffer

import scalaz._

class IngestServiceHandler(accessControl: AccessControl[Future], eventStore: EventStore, insertTimeout: Timeout, threadPool: Executor, maxBatchErrors: Int)(implicit dispatcher: MessageDispatcher)
extends CustomHttpService[Either[Future[JValue], ByteChunk], (APIKeyRecord, Path, AccountID) => Future[HttpResponse[JValue]]] with Logging {

  protected implicit val M = new FutureMonad(ExecutionContext.fromExecutor(threadPool))

  def writeChunkStream(chan: WritableByteChannel, chunk: ByteChunk): Future[Unit] = {
    def writeChannel(stream: StreamT[Future, ByteBuffer]): Future[Unit] = {
      stream.uncons flatMap {
        case Some((bb, tail)) => { chan.write(bb); writeChannel(tail) }
        case None => Future(chan.close())
      }
    }

    chunk match {
      case Left(bb) => Future { chan.write(bb); chan.close() }
      case Right(stream) => writeChannel(stream)
    }
  }

  def ingest(r: APIKeyRecord, p: Path, accountId: AccountID, event: JValue): Future[Unit] = {
    val eventInstance = Event.fromJValue(r.apiKey, p, Some(accountId), event)
    logger.trace("Saving event: " + eventInstance)
    eventStore.save(eventInstance, insertTimeout)
  }

  case class SyncResult(total: Int, ingested: Int, errors: List[(Int, String)])

  class EventQueueInserter(t: APIKeyRecord, p: Path, o: AccountID, events: Iterator[Either[String, JValue]], close: Option[Closeable]) extends Runnable {
    private[service] val result: Promise[SyncResult] = Promise()

    def run() {
      val errors: ListBuffer[(Int, String)] = new ListBuffer()
      val futures: ListBuffer[Future[Unit]] = new ListBuffer()
      var i = 0
      while (events.hasNext) {
        val ev = events.next()
        if (errors.size < maxBatchErrors) {
          ev match {
            case Right(event) => futures += ingest(t, p, o, event)
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

  def parseCsv(byteStream: ByteChunk, t: APIKeyRecord, p: Path, o: AccountID, readCsv: File => CSVReader): Future[EventQueueInserter] = {
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

      new EventQueueInserter(t, p, o, jVals, Some(csv))
    }
  }

  def parseJson(channel: ReadableByteChannel, t: APIKeyRecord, p: Path, o: AccountID): EventQueueInserter = {
    val reader = new BufferedReader(Channels.newReader(channel, "UTF-8"))
    val lines = Iterator.continually(reader.readLine()).takeWhile(_ != null)
    new EventQueueInserter(t, p, o, lines map { json =>
      try {
        Right(JParser.parse(json))
      } catch {
        case e => Left("Parsing failed: " + e.getMessage())
      }
    }, Some(reader))
  }

  def parseSyncJson(byteStream: ByteChunk, t: APIKeyRecord, p: Path, o: AccountID): Future[EventQueueInserter] = {
    val pipe = Pipe.open()
    for {
      _ <- writeChunkStream(pipe.sink(), byteStream)
    } yield parseJson(pipe.source(), t, p, o)
  }

  def parseAsyncJson(byteStream: ByteChunk, t: APIKeyRecord, p: Path, o: AccountID): Future[EventQueueInserter] = {
    for {
      file <- writeToFile(byteStream)
    } yield {
      parseJson(new FileInputStream(file).getChannel(), t, p, o)
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
    Success { (r: APIKeyRecord, p: Path, o: AccountID) =>
      accessControl.hasCapability(r.apiKey, Set(WritePermission(p, Set())), None) flatMap {
        case true => try {
          logger.debug("Ingesting events as: APIKey: "+r.apiKey+" path: "+p+" account id: "+o)
          request.content map {
            case Left(futureEvent) =>
              for {
                event <- futureEvent
                _ <- ingest(r, p, o, event)
              } yield HttpResponse[JValue](OK)

            case Right(byteStream) =>
              import MimeTypes._
              import Validation._

              val async = request.parameters.get('sync) map (_ == "async") getOrElse false
              val parser = if (request.mimeTypes contains (text / csv)) {
                csvReaderFor(request) map (parseCsv(byteStream, r, p, o, _))
              } else if (async) {
                success(parseAsyncJson(byteStream, r, p, o))
              } else  {
                success(parseSyncJson(byteStream, r, p, o))
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
