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
package service

import util._

import com.precog.common.Path
import com.precog.common.accounts._
import com.precog.common.client._
import com.precog.common.ingest._
import com.precog.common.jobs._
import com.precog.common.security._
import com.precog.util.PrecogUnit

import akka.dispatch.{ExecutionContext, Future, Promise}
import akka.dispatch.ExecutionContext
import akka.util.Timeout

import blueeyes.bkka._
import blueeyes.core.data.ByteChunk
import blueeyes.core.http._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.http.HttpHeaders._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.Extractor
import blueeyes.util.Clock

import com.google.common.base.Charsets

import java.io.{ File, FileReader, BufferedReader, FileInputStream, FileOutputStream, Closeable, InputStreamReader }
import java.nio.channels._
import java.nio.ByteBuffer
import java.util.concurrent.{ Executor, RejectedExecutionException }

import com.weiglewilczek.slf4s.Logging

import au.com.bytecode.opencsv.CSVReader

import scalaz._
import scalaz.\/._
import scalaz.Validation._
import scalaz.std.function._
import scalaz.std.option._
import scalaz.syntax.id._
import scalaz.syntax.arrow._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._
import scala.annotation.tailrec

class IngestServiceHandler(
    permissionsFinder: PermissionsFinder[Future],
    jobManager: JobManager[Response],
    clock: Clock,
    eventStore: EventStore[Future],
    ingestTimeout: Timeout,
    batchSize: Int)(implicit M: Monad[Future], executor: ExecutionContext)
    extends CustomHttpService[ByteChunk, (APIKey, Path) => Future[HttpResponse[JValue]]]
    with Logging {

  sealed trait ParseDirective {
    def toMap: Map[String, String] // escape hatch for interacting with other systems
  }

  case class CSVDelimiter(delimiter: String) extends ParseDirective { val toMap = Map("csv:delimiter" -> delimiter) }
  case class CSVQuote(quote: String) extends ParseDirective { val toMap = Map("csv:quote" -> quote) }
  case class CSVSeparator(separator: String) extends ParseDirective { val toMap = Map("csv:separator" -> separator) }
  case class CSVEscape(escape: String) extends ParseDirective { val toMap = Map("csv:escape" -> escape) }
  case class MimeDirective(mimeType: MimeType) extends ParseDirective { val toMap = Map("content-type" -> mimeType.toString) }


  sealed trait IngestResult
  case class AsyncSuccess(contentLength: Long) extends IngestResult
  case class BatchSyncResult(total: Int, ingested: Int, errors: Vector[(Int, String)]) extends IngestResult
  case class StreamingSyncResult(ingested: Int, error: Option[String]) extends IngestResult
  case class NotIngested(reason: String) extends IngestResult

  private val excessiveFieldsError = "Cannot ingest values with more than 250 primitive fields. This limitiation will be lifted in a future release. Thank you for your patience."

  trait BatchIngest {
    def apply(data: ByteChunk, parseDirectives: Set[ParseDirective], jobId: JobId, sync: Boolean): Future[IngestResult]
  }

  class JSONBatchIngest(apiKey: APIKey, path: Path, authorities: Authorities) extends BatchIngest {
    @tailrec final def readBatch(reader: BufferedReader, batch: Vector[Validation[Throwable, JValue]]): Vector[Validation[Throwable, JValue]] = {
      val line = reader.readLine()

      if (line == null || batch.size >= batchSize) batch
      else if (line.trim.isEmpty) readBatch(reader, batch)
      else readBatch(reader, batch :+ JParser.parseFromString(line))
    }

    def ingestSync(channel: ReadableByteChannel, jobId: JobId): Future[IngestResult] = {
      def readBatches(reader: BufferedReader, total: Int, ingested: Int, errors: Vector[(Int, String)]): Future[IngestResult] = {
        M.point(readBatch(reader, Vector())) flatMap { batch =>
          if (batch.isEmpty) {
            // the batch will only be empty if there's nothing left to read
            // TODO: notify jobs api of completion
            M.point(BatchSyncResult(total, ingested, errors))
          } else {
            val (_, values, errors0) = batch.foldLeft((0, Vector.empty[JValue], Vector.empty[(Int, Extractor.Error)])) {
              case ((i, values, errors), Success(value)) if value.flattenWithPath.size < 250 =>
                (i + 1, values :+ value, errors)
              case ((i, values, errors), Success(value)) =>
                (i + 1, values, errors :+ (i, Extractor.Invalid(excessiveFieldsError)))
              case ((i, values, errors), Failure(error)) =>
                (i + 1, values, errors :+ (i, Extractor.Thrown(error)))
            }

            ingest(apiKey, path, authorities, values, Some(jobId)) flatMap { _ =>
              readBatches(reader, total + batch.length, ingested + values.length, errors ++ (errors0 map { ((_:Extractor.Error).message).second }))
            }
          }
        }
      }

      readBatches(new BufferedReader(Channels.newReader(channel, "UTF-8")), 0, 0, Vector())
    }

    def apply(data: ByteChunk, parseDirectives: Set[ParseDirective], jobId: JobId, sync: Boolean): Future[IngestResult] = {
      if (sync) {
        val pipe = Pipe.open()
        val channel = pipe.sink()
        val writeFuture = writeChunkStream(channel, data)
        val readFuture = ingestSync(pipe.source(), jobId)
        M.apply2(writeFuture, readFuture) { (written, result) => result }
      } else {
        for ((file, size) <- writeToFile(data)) yield {
          ingestSync(new FileInputStream(file).getChannel(), jobId)
          AsyncSuccess(size)
        }
      }
    }
  }

  class CSVBatchIngest(apiKey: APIKey, path: Path, authorities: Authorities) extends BatchIngest {
    import scalaz.syntax.applicative._
    import scalaz.Validation._

    def readerBuilder(parseDirectives: Set[ParseDirective]): ValidationNEL[String, java.io.Reader => CSVReader] = {
      def charOrError(s: Option[String], default: Char): ValidationNEL[String, Char] = {
        s map {
          case s if s.length == 1 => success(s.charAt(0))
          case _ => failure("Expected a single character but found a string.")
        } getOrElse {
          success(default)
        } toValidationNEL
      }

      val delimiter = charOrError(parseDirectives collectFirst { case CSVDelimiter(str) => str }, ',')
      val quote     = charOrError(parseDirectives collectFirst { case CSVQuote(str) => str }, '"')
      val escape    = charOrError(parseDirectives collectFirst { case CSVEscape(str) => str }, '\\')

      (delimiter |@| quote |@| escape) { (delimiter, quote, escape) =>
        (reader: java.io.Reader) => new CSVReader(reader, delimiter, quote, escape)
      }
    }

    @tailrec final def readBatch(reader: CSVReader, batch: Vector[Array[String]]): Vector[Array[String]] = {
      val nextRow = reader.readNext()
      if (nextRow == null || batch.size >= batchSize) batch else readBatch(reader, batch :+ nextRow)
    }

    def ingestSync(reader: CSVReader, jobId: JobId): Future[IngestResult] = {
      def readBatches(paths: Array[JPath], reader: CSVReader, total: Int, ingested: Int, errors: Vector[(Int, String)]): Future[IngestResult] = {
        // TODO: handle errors in readBatch
        M.point(readBatch(reader, Vector())) flatMap { batch =>
          if (batch.isEmpty) {
            // the batch will only be empty if there's nothing left to read
            // TODO: Write out job completion information to the queue.
            M.point(BatchSyncResult(total, ingested, errors))
          } else {
            val types = CsvType.inferTypes(batch.iterator)
            val jvals = batch map { row =>
              (paths zip types zip row).foldLeft(JUndefined: JValue) { case (obj, ((path, tpe), s)) =>
                JValue.unsafeInsert(obj, path, tpe(s))
              }
            }

            ingest(apiKey, path, authorities, jvals, Some(jobId)) flatMap { _ =>
              readBatches(paths, reader, total + batch.length, ingested + batch.length, errors)
            }
          }
        }
      }

      M.point(reader.readNext()) flatMap { header =>
        if (header == null) {
          M.point(NotIngested("No CSV data was found in the request content."))
        } else {
          readBatches(header.map(JPath(_)), reader, 0, 0, Vector())
        }
      }
    }

    def apply(data: ByteChunk, parseDirectives: Set[ParseDirective], jobId: JobId, sync: Boolean): Future[IngestResult] = {
      readerBuilder(parseDirectives) map { f =>
        if (sync) {
          // must not return until everything is persisted to kafka central
          val pipe = Pipe.open()
          val channel = pipe.sink()
          val writeFuture = writeChunkStream(channel, data)
          val readFuture = ingestSync(f(new BufferedReader(Channels.newReader(pipe.source(), "UTF-8"))), jobId)
          M.apply2(writeFuture, readFuture) { (written, result) => result }
        } else {
          for ((file, size) <- writeToFile(data)) yield {
            // spin off a future, but don't bother flatmapping through it since we
            // can return immediately
            ingestSync(f(new InputStreamReader(new FileInputStream(file), "UTF-8")), jobId)
            AsyncSuccess(size)
          }
        }
      } valueOr { errors =>
        M.point(NotIngested(errors.list.mkString("; ")))
      }
    }
  }

  /** Chain of responsibility used to determine a BatchIngest strategy */
  trait BatchIngestSelector {
    def select(partialData: Array[Byte], parseDirectives: Set[ParseDirective]): Option[BatchIngest]
  }

  class MimeBatchIngestSelector(apiKey: APIKey, path: Path, authorities: Authorities) extends BatchIngestSelector {
    def select(partialData: Array[Byte], parseDirectives: Set[ParseDirective]): Option[BatchIngest] = {
      val JSON = application/json
      val CSV = text/csv

      parseDirectives collectFirst {
        case MimeDirective(JSON) => new JSONBatchIngest(apiKey, path, authorities)
        case MimeDirective(CSV) => new CSVBatchIngest(apiKey, path, authorities)
      }
    }
  }

  class JsonBatchIngestSelector(apiKey: APIKey, path: Path, authorities: Authorities) extends BatchIngestSelector {
    def select(partialData: Array[Byte], parseDirectives: Set[ParseDirective]): Option[BatchIngest] = {
      val (AsyncParse(errors, values), parser) = JParser.parseAsync(ByteBuffer.wrap(partialData))
      (errors.isEmpty && !values.isEmpty) option { new JSONBatchIngest(apiKey, path, authorities) }
    }
  }

  def batchSelectors(apiKey: APIKey, path: Path, authorities: Authorities): List[BatchIngestSelector] = List(
    new MimeBatchIngestSelector(apiKey, path, authorities),
    new JsonBatchIngestSelector(apiKey, path, authorities)
  )

  def ensureByteBufferSanity(bb: ByteBuffer) = {
    if (bb.remaining == 0) {
      // Oh hell no. We don't need no stinkin' pre-read ByteBuffers
      bb.rewind()
    }
  }

  final private def writeChannel(chan: WritableByteChannel, stream: StreamT[Future, ByteBuffer], written: Long): Future[Long] = {
    stream.uncons flatMap {
      case Some((buf, tail)) =>
        ensureByteBufferSanity(buf)
        val written0 = chan.write(buf)
        writeChannel(chan, tail, written + written0)

      case None =>
        M.point { chan.close(); written }
    }
  }

  def writeChunkStream(chan: WritableByteChannel, chunk: ByteChunk): Future[Long] = {
    chunk match {
      case Left(bb) => writeChannel(chan, bb :: StreamT.empty[Future, ByteBuffer], 0L)
      case Right(stream) => writeChannel(chan, stream, 0L)
    }
  }

  def writeToFile(byteStream: ByteChunk): Future[(File, Long)] = {
    val file = File.createTempFile("async-ingest-", null)
    val outChannel = new FileOutputStream(file).getChannel()
    for (written <- writeChunkStream(outChannel, byteStream)) yield (file, written)
  }

  def ingest(apiKey: APIKey, path: Path, authorities: Authorities, data: Seq[JValue], jobId: Option[JobId]): Future[PrecogUnit] = {
    val eventInstance = Ingest(apiKey, path, Some(authorities), data, jobId, clock.instant())
    logger.trace("Saving event: " + eventInstance)
    eventStore.save(eventInstance, ingestTimeout)
  }

  def getParseDirectives(request: HttpRequest[_]): Set[ParseDirective] = {
    val mimeDirective =
      for {
        header <- request.headers.header[`Content-Type`]
        mimeType <- header.mimeTypes.headOption
      } yield MimeDirective(mimeType)

    val delimiter = request.parameters get 'delimiter map { CSVDelimiter(_) }
    val quote = request.parameters get 'quote map { CSVQuote(_) }
    val escape = request.parameters get 'escape map { CSVEscape(_) }

    mimeDirective.toSet ++ delimiter ++ quote ++ escape
  }

  @tailrec final def selectBatchIngest(from: List[BatchIngestSelector], partialData: Array[Byte], parseDirectives: Set[ParseDirective]): Option[BatchIngest] = {
    from match {
      case hd :: tl =>
        hd.select(partialData, parseDirectives) match { // not using map so as to get tailrec
          case None => selectBatchIngest(tl, partialData, parseDirectives)
          case some => some
        }

      case Nil => None
    }
  }

  def ingestBatch(apiKey: APIKey, path: Path, authorities: Authorities, data: ByteChunk, parseDirectives: Set[ParseDirective], batchJob: JobId, sync: Boolean): Future[IngestResult] = {
    def array(buffer: ByteBuffer): Array[Byte] = {
      val target = new Array[Byte](buffer.remaining)
      buffer.get(target)
      buffer.flip()
      target
    }

    val selectors = batchSelectors(apiKey, path, authorities)

    val futureBatchIngest = data match {
      case Left(buf) =>
        M.point(selectBatchIngest(selectors, array(buf), parseDirectives))

      case Right(stream) =>
        stream.uncons map {
          _ flatMap { case (buf, _) => selectBatchIngest(selectors, array(buf), parseDirectives) }
        }
    }

    futureBatchIngest flatMap {
      case Some(batchIngest) => batchIngest(data, parseDirectives, batchJob, sync)
      case None => M.point(NotIngested("Could not successfully determine a data type for your batch ingest. Please consider setting the Content-Type header."))
    }
  }

  def ingestStreaming(apiKey: APIKey, path: Path, authorities: Authorities, data: ByteChunk, parseDirectives: Set[ParseDirective]): Future[IngestResult] = {
    def ingestAll(channel: ReadableByteChannel): Future[IngestResult] = {
      def read(reader: BufferedReader, ingested: Int): Future[IngestResult] = {
        M.point(reader.readLine()) flatMap { line =>
          if (line == null) Promise successful StreamingSyncResult(ingested, None)
          else if (line.trim.isEmpty) read(reader, ingested)
          else {
            JParser.parseFromString(line) map { jvalue =>
              if (jvalue.flattenWithPath.size > 250) {
                Promise successful StreamingSyncResult(ingested, Some(excessiveFieldsError))
              } else {
                ingest(apiKey, path, authorities, Vector(jvalue), None) flatMap { _ => read(reader, ingested + 1) }
              }
            } valueOr { error =>
              logger.warn("Ingest for %s with key %s at %s failed after %d results!".format(authorities, apiKey, path.toString, ingested), error)
              Promise successful StreamingSyncResult(ingested, Some(error.getMessage))
            }
          }
        }
      }

      // must lift the read into the future to avoid blocking on the initial read
      read(new BufferedReader(Channels.newReader(channel, "UTF-8")), 0)
    }

    data match {
      case Left(buf) =>
        ensureByteBufferSanity(buf)
        val readableLength = buf.remaining
        JParser.parseManyFromByteBuffer(buf) map { jvalues =>
          if (jvalues.exists(_.flattenWithPath.size > 250)) {
            Promise successful NotIngested(excessiveFieldsError)
          } else {
            ingest(apiKey, path, authorities, jvalues, None) map { _ => StreamingSyncResult(jvalues.length, None) }
          }
        } valueOr { error =>
          val message = error match {
            case e: IndexOutOfBoundsException => "Input exhausted during parse"
            case o => Option(o.getMessage).getOrElse(o.getClass.toString)
          }
          logger.warn("Ingest for %s with key %s at %s failed!".format(authorities, apiKey, path.toString), error)
          Promise successful NotIngested(message)
        }

      case Right(stream) =>
        val pipe = Pipe.open()
        val channel = pipe.sink()
        val writeFuture = writeChannel(channel, stream, 0)
        val readFuture = ingestAll(pipe.source())
        M.apply2(writeFuture, readFuture) { (written, result) => result }
    }
  }

  val service: HttpRequest[ByteChunk] => Validation[NotServed, (APIKey, Path) => Future[HttpResponse[JValue]]] = (request: HttpRequest[ByteChunk]) => {
    logger.debug("Got request in ingest handler: " + request)
    Success { (apiKey: APIKey, path: Path) => {
      val timestamp = clock.now()
      def createJob = jobManager.createJob(apiKey, "ingest-" + path, "ingest", None, Some(timestamp)) map { _.id }

      val requestAuthorities = for {
        paramIds <- request.parameters.get('ownerAccountId) 
        auths <- paramIds.split("""\s*,\s*""") |> { ids => if (ids.isEmpty) None else Authorities.ifPresent(ids.toSet) } 
      } yield auths

      // FIXME: Provisionally accept data for ingest if one of the permissions-checking services is unavailable
      requestAuthorities map { authorities => 
        permissionsFinder.checkWriteAuthorities(authorities, apiKey, path, timestamp.toInstant) map { _.option(authorities) }
      } getOrElse {
        permissionsFinder.inferWriteAuthorities(apiKey, path, Some(timestamp.toInstant)) 
      } flatMap {
        case Some(authorities) =>
          logger.debug("Write permission granted for " + authorities + " to " + path)
          request.content map { content =>
            import MimeTypes._
            import Validation._

            val parseDirectives = getParseDirectives(request)
            val batchMode = request.parameters.get('mode) exists (_ equalsIgnoreCase "batch")
            // assign new job ID for batch-mode queries only
            for {
              batchJob <- batchMode.option(createJob.run).sequence
              ingestResult <- (batchJob map {
                                _ map { jobId =>
                                  val sync = request.parameters.get('receipt) match {
                                    case Some(v) => v equalsIgnoreCase "true"
                                    case None => request.parameters.get('sync) forall (_ equalsIgnoreCase "sync")
                                  }

                                  ingestBatch(apiKey, path, authorities, content, parseDirectives, jobId, sync)
                                }
                              } getOrElse {
                                right(ingestStreaming(apiKey, path, authorities, content, parseDirectives))
                              }).sequence
            } yield {
              ingestResult.fold(
                { message =>
                  logger.warn("Internal error during ingest: " + message)
                  HttpResponse[JValue](InternalServerError, content = Some(JString("An error occurred creating a batch ingest job: " + message)))},
                {
                  case NotIngested(reason) =>
                    logger.warn("Ingest failed to %s with %s with reason: %s ".format(path, apiKey, reason))
                    HttpResponse[JValue](BadRequest, content = Some(JString(reason)))

                  case AsyncSuccess(contentLength) =>
                    logger.info("Async ingest succeeded to %s with %s, length %d".format(path, apiKey, contentLength))
                    HttpResponse[JValue](Accepted, content = Some(JObject(JField("content-length", JNum(contentLength)) :: Nil)))

                  case BatchSyncResult(total, ingested, errors) =>
                    val failed = errors.size
                    val responseContent = JObject(
                      JField("total", JNum(total)),
                      JField("ingested", JNum(ingested)),
                      JField("failed", JNum(failed)),
                      JField("skipped", JNum(total - ingested - failed)),
                      JField("errors", JArray(errors map { case (line, msg) => JObject(JField("line", JNum(line)) :: JField("reason", JString(msg)) :: Nil) }: _*))
                    )

                    logger.info("Batch sync ingest succeeded to %s with %s. Result: %s".format(path, apiKey, responseContent.renderPretty))

                    if (ingested == 0 && total > 0) {
                      HttpResponse[JValue](BadRequest, content = Some(responseContent))
                    } else {
                      HttpResponse[JValue](OK, content = Some(responseContent))
                    }

                  case StreamingSyncResult(ingested, error) =>
                    val responseContent = JObject(JField("ingested", JNum(ingested)), JField("errors", JArray(error.map(JString(_)).toList)))
                    val responseCode = if (error.isDefined) { if (ingested == 0) BadRequest else RetryWith } else OK
                    logger.info("Streaming sync ingest succeeded to %s with %s. Result: %s".format(path, apiKey, responseContent.renderPretty))
                    HttpResponse(responseCode, content = Some(responseContent))
                }
              )
            }
          } getOrElse {
            logger.warn("No event data found for ingest request from %s owner %s at path %s".format(apiKey, authorities, path))
            M.point(HttpResponse[JValue](BadRequest, content = Some(JString("Missing event data."))))
          }

        case None =>
          logger.warn("Unable to resolve accounts for write from %s owners %s to path %s".format(apiKey, request.parameters.get('ownerAccountId), path))
          M.point(HttpResponse[JValue](Forbidden, content = Some(JString("Either the ownerAccountId parameter you specified could not be resolved to a set of permitted accounts, or the API key specified was invalid."))))
      }
    }}
  }

  val metadata = Some(DescriptionMetadata(
    """
      This service can be used to store an data point with or without an associated timestamp.
      Timestamps are not added by default.
    """
  ))
}
