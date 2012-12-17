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
import com.precog.util.PrecogUnit
import heimdall._

import blueeyes.bkka._
import akka.dispatch.{ExecutionContext, Future, Promise}
import akka.dispatch.ExecutionContext
import akka.util.Timeout

import blueeyes.core.data.ByteChunk
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.util.Clock

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

class IngestServiceHandler(
    accessControl: AccessControl[Future], 
    jobManager: JobManager[Future], 
    clock: Clock, 
    eventStore: EventStore, 
    insertTimeout: Timeout, 
    batchSize: Int,
    maxBatchErrors: Int)(implicit executor: ExecutionContext)
    extends CustomHttpService[ByteChunk, (APIKeyRecord, Path, Account) => Future[HttpResponse[ByteChunk]]] 
    with Logging {

  sealed trait IngestResult
  case class AsyncSuccess(contentLength: Long) extends IngestResult
  case class SyncSuccess(total: Int, ingested: Int, errors: Vector[(Int, String)]) extends IngestResult
  case class NotIngested(reason: String) extends IngestResult

  sealed trait ParseDirective {
    def toMap: Map[String, String] // escape hatch for interacting with other systems
  }

  case class CSVDelimiter(delimiter: String) extends ParseDirective { val toMap = Map("csv:delimiter" -> delimiter) }
  case class CSVQuote(quote: String) extends ParseDirective { val toMap = Map("csv:quote" -> separator) }
  case class CSVSeparator(separator: String) extends ParseDirective { val toMap = Map("csv:separator" -> separator) }
  case class CSVEscape(escape: String) extends ParseDirective { val toMap = Map("csv:escape" -> escape) }
  case class MimeDirective(mimeType: MimeType) extends ParseDirecive { val toMap = Map("content-type" -> mimeType.toString) }

  trait BatchIngestSelector {
    def select(partialData: Array[Byte], parseDirectives: Set[ParseDirective]): Option[BatchIngest]
  }

  class MimeBatchIngestSelector(t: APIKeyRecord, p: Path, o: Account) extends BatchIngestSelector {
    def select(partialData: Array[Byte], parseDirectives: Set[ParseDirective]): Option[BatchIngest] = {
      val JSON = application/json
      val CSV = text/csv

      parseDirectives collectFirst {
        case MimeDirective(JSON) => new JSONBatchIngest(t, p, o)
        case MimeDirective(CSV) => new CSVBatchIngest(t, p, o)
      }
    }
  }

  class JsonBatchIngestSelector(t: APIKeyRecord, p: Path, o: Account) extends BatchIngestSelector {
    def select(partialData: Array[Byte], parseDirectives: Set[ParseDirective]): Option[BatchIngest] = {
      val (AsyncParse(errors, values), parser) = JParser.parseAsync(ByteBuffer.wrap(partialData)) 
      (errors.isEmpty && !values.isEmpty) option { new JSONBatchIngest(t, p, o) }
    }
  }

  trait BatchIngest {
    def apply(data: ByteChunk, parseDirectives: Set[ParseDirective], job: JobId, sync: Boolean): Future[IngestResult]
  }

  class JSONBatchIngest(apiKey: APIKeyRecord, path: Path, account: Account) extends BatchIngest {
    @tailrec def readBatch(reader: BufferedReader, batch: Vector[Validation[Throwable, JValue]]): Vector[Validation[Throwable, JValue]] = {
      val line = reader.readLine()
      if (line == null || batch.size >= batchSize) batch 
      else readBatch(reader, batch :+ JParser.parseFromString(line))
    }

    def ingestSync(channel: ReadableByteChannel, jobId: JobId): Future[IngestResult] = {
      def readBatches(reader: BufferedReader, total: Int, ingested: Int, errors: Vector[(Int, String)]): Future[IngestResult] = {
        val batch = readBatch(reader)
        if (batch.isEmpty) {
          // the batch will only be empty if there's nothing left to read
          // TODO: Write out job completion information to the queue.
          Promise.successful(SyncSuccess(total, ingested, errors))
        } else {
          val (values, errors0) = batch.foldLeft((Vector.empty[JValue], Vector.empty[Throwable])) {
            case ((values, errors), Success(value)) => (values :+ value, errors)
            case ((values, errors), Failure(value)) => (values, errors :+ error)
          }

          ingest(apiKey, path, account, values, Some(jobId)) flatMap { _ =>
            readBatches(reader, total + batch.length, ingest + values.length, errors ++ errors0)
          }
        }
      }

      readBatches(new BufferedReader(Channels.newReader(channel, "UTF-8")), 0, 0, Vector())
    }

    def apply(data: ByteChunk, parseDirectives: Set[ParseDirective], job: JobId, sync: Boolean): Future[IngestResult] = {
      if (sync) {
        val pipe = Pipe.open()
        writeChunkStream(pipe.sink(), byteStream)
        ingestSync(pipe.source())
      } else {
        for ((file, size) <- writeToFile(byteStream)) yield {
          ingestSync(new FileInputStream(file).getChannel(), jobId)
          AsyncSuccess(size)
        } 
      }
    }
  }

  class CSVBatchIngest(apiKey: APIKeyRecord, path: Path, account: Account) extends BatchIngest {
    import scalaz.syntax.applicative._
    import scalaz.Validation._

    def readerBuilder(parseDirectives: Set[ParseDirective]): ValidationNEL[String, Reader => CSVReader] = {
      def charOrError(s: Option[String], default: Char): Validation[String, Char] = s map {
        case s if s.length == 1 => success(s.charAt(0))
        case _ => failure("Expected a single character but found a string.")
      } getOrElse success(default)

      val delimiter = charOrError(parseDirectives collectFirst { case CSVDelimiter(str) => str }, ',')
      val quote     = charOrError(parseDirectives collectFirst { case CSVQuote(str) => str }, '"')
      val escape    = charOrError(parseDirectives collectFirst { case CSVEscape(str) => str }, '\\')

      (delimiter |@| quote |@| escape) { (delimiter, quote, escape) => 
        (reader: Reader) => new CSVReader(reader, delimiter, quote, escape)
      }
    }

    @tailrec def readBatch(reader: CSVReader, batch: Vector[Array[String]]): Vector[Array[String]] = {
      val nextRow = csv.readNext()
      if (nextRow == null || batch.size >= batchSize) batch else readBatch(reader, batch :+ nextRow)
    }

    def ingestSync(reader: CSVReader, jobId: JobId): Future[IngestResult] = {
      def readBatches(paths: Array[JPath], reader: CSVReader, total: Int, ingested: Int, errors: Vector[(Int, String)]): Future[IngestResult] = {
        // TODO: handle errors in readBatch
        val batch = readBatch(reader)
        if (batch.isEmpty) {
          // the batch will only be empty if there's nothing left to read
          // TODO: Write out job completion information to the queue.
          Promise.successful(SyncSuccess(total, ingested, errors))
        } else {
          val types = CsvType.inferTypes(batch.iterator)
          val jVals = batch map { row =>
            (paths zip types zip row).foldLeft(JUndefined: JValue) { case (obj, ((path, tpe), s)) =>
              JValue.unsafeInsert(obj, path, tpe(s))
            }
          }

          ingest(apiKey, path, account, jvals, Some(jobId)) flatMap { _ => 
            readBatches(paths, reader, total + batch.length, ingested + batch.length, errors)
          }
        }
      }

      val header = reader.readNext()
      if (header == null) {
        Promise.successful(NotIngested("No CSV data was found in the request content."))
      } else {
        readBatches(header.map(JPath(_)), reader, 0, 0, Vector())
      }
    }

    def apply(data: ByteChunk, parseDirectives: Set[ParseDirective], job: JobId, sync: Boolean): Future[IngestResult]
      readerBuilder(parseDirectives) map { f =>
      if (sync) {
        // must not return until everything is persisted to kafka central
        val pipe = Pipe.open()
        writeChunkStream(pipe.sink(), data)
        ingestSync(f(new BufferedReader(Channels.newReader(pipe.source(), "UTF-8"))))
      } else {
        for ((file, size) <- writeToFile(byteStream)) yield {
          // spin off a future, but don't bother flatmapping through it since we
          // can return immediately
          ingestSync(f(new InputStreamReader(new FileInputStream(file), "UTF-8")))
          AsyncSuccess(size)
        }
      }
    }
  }

  protected implicit val M = new FutureMonad(ExecutionContext.fromExecutor(threadPool))

  def ensureByteBufferSanity(bb: ByteBuffer) = {
    if (bb.remaining == 0) {
      // Oh hell no. We don't need no stinkin' pre-read ByteBuffers
      bb.rewind()
    }
  }

  def writeChunkStream(chan: WritableByteChannel, chunk: ByteChunk): Future[Long] = {
    def writeChannel(stream: StreamT[Future, ByteBuffer], written: Long): Future[Long] = {
      stream.uncons flatMap {
        case Some((bb, tail)) => 
          ensureByteBufferSanity(bb)
          val written0 = chan.write(bb)
          writeChannel(tail, written + written0)

        case None => 
          Future { chan.close(); written }
      }
    }

    chunk match {
      case Left(bb) => writeChannel(bb :: StreamT.empty[Future, ByteBuffer], 0L)
      case Right(stream) => writeChannel(stream, 0L)
    }
  }

  def writeToFile(byteStream: ByteChunk): Future[(File, Long)] = {
    val file = File.createTempFile("async-ingest-", null)
    val outChannel = new FileOutputStream(file).getChannel()
    for (written <- writeChunkStream(outChannel, byteStream)) yield (file, written)
  }

  def ingest(r: APIKeyRecord, p: Path, account: Account, data: Seq[JValue], jobId: Option[JobId]): Future[Unit] = {
    val eventInstance = Event(r.apiKey, p, Some(account.accountId), data, Map(), jobId)
    logger.trace("Saving event: " + eventInstance)
    eventStore.save(eventInstance, insertTimeout)
  }

  def getParseDirectives(request: HttpRequest[_]): Set[ParseDirective] = {
    val mimeDirective = for {
                          header <- request.headers.header[`Content-Type`] 
                          t <- header.mimeTypes.headOption
                        } yield MimeDirective(t)

    val delimiter = request.parameters get 'delimiter map CSVDelimiter(_)
    val quote = request.parameters get 'quote map CSVQuote(_)
    val escape = request.parameters get 'escape map CSVEscape(_)

    Set(mimeDirective, delimiter, quote, escape).flatten
  }

  @tailrec def selectBatchIngest(from: List[BatchIngestSelector], partialData: Array[Byte], parseDirectives: Set[ParseDirective]): Option[BatchIngest] = {
    from match {
      case hd :: tl => 
        hd.select(partialData, parseDirectives) match { // not using map so as to get tailrec
          case None => selectBatchIngest(tl, partialData, parseDirectives)
          case some => some
        }

      case Nil => None
    }
  }

  def ingestBatch(data: ByteChunk, parseDirectives: Set[ParseDirective], batchJob: JobId, sync: Boolean): Future[IngestResult] = {
    def array(buffer: ByteBuffer): Array[Byte] = {
      val target = new Array[Byte](buf.remaining)
      buf.get(target)
      buf.flip()
      target
    }

    MimeBatchIngestSelector.select(Array[Byte](), parseDirectives) map { i => Promise.successful(Some(i)) } getOrElse {
      data match {
        case Left(buf) => 
          Promise.successful(selectBatchIngest(contentBatchSelectors, array(buf), parseDirectives))

        case Right(stream) => 
          stream.uncons map { 
            _ map { buf => selectBatchIngest(contentBatchSelectors, array(buf), parseDirectives) }
          }
      }
    } flatMap {
      case Some(batchIngest) => batchIngest(data, parseDirectives, batchJob, sync)
      case None => Promise.successful(NotIngested("Could not successfully determine a data type for your batch ingest. Please consider setting the Content-Type header."))
    }
  }

  val service = (request: HttpRequest[ByteChunk]) => {
    Success { (r: APIKeyRecord, p: Path, o: Account) =>
      accessControl.hasCapability(r.apiKey, Set(WritePermission(p, Set())), None) flatMap {
        case true => 
          request.content map { content =>
            import MimeTypes._
            import Validation._

            val parseDirectives = getParseDirectives(request)
            val batchMode = request.parameters.get('mode) exists (_ equalsIgnoreCase "batch") 
            val sync = request.parameters.get('receipt) exists (_ equalsIgnoreCase "true")

            // assign new job ID for batch-mode queries only
            val batchJobId: Option[Future[JobId]] = 
            for {
              batchJob <- batchMode.option(jobManager.createJob(r.apiKey, "ingest-" + p, "ingest", Some(clock.now()), None).jobId).sequence
              ingestResult <- batchJob map { jobId =>
                                ingestBatch(content, parseDirectives, jobId, sync)
                              } getOrElse {
                                ingestStreaming(content, parseDirectives, sync)
                              }
            } yield {
              ingestResult match {
                case NotIngested(reason) =>
                  HttpResponse[JValue](BadRequest, content = Some(JString(reason)))

                case AsyncResult(contentLength) =>
                  HttpResponse[JValue](Accepted, content = Some(JObject(JField("content-length", contentLength) :: Nil))
              
                case SyncResult(total, ingested, errors) =>
                  if (ingested == 0 && total > 0) {
                  } else {
                    val failed = errors.size
                    val responseContent = JObject(
                      JField("total", JNum(total)) :: 
                      JField("ingested", JNum(ingested)) ::
                      JField("failed", JNum(failed)) ::
                      JField("skipped", JNum(total - ingested - failed)) ::
                      JField("errors", JArray(errors map { case (line, msg) => JObject(JField("line", JNum(line)) :: JField("reason", JString(msg)) :: Nil)) }) :: 
                      Nil
                    )

                    HttpResponse[JValue](OK, content = Some(responseContent))
                  }
              }
            }
          } getOrElse {
            Future(HttpResponse[JValue](BadRequest, content = Some(JString("Missing event data."))))
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
