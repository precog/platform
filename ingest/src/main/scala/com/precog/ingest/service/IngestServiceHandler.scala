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
import com.precog.yggdrasil.actor.IngestErrors

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

import java.io._
import java.nio.channels.{Channels, ReadableByteChannel, WritableByteChannel}
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
  batchSize: Int,
  maxFields: Int)(implicit M: Monad[Future], executor: ExecutionContext)
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
  case object AsyncSuccess extends IngestResult
  case class BatchSyncResult(total: Int, ingested: Int, errors: Vector[(Int, String)]) extends IngestResult
  case class StreamingSyncResult(ingested: Int, error: Option[String]) extends IngestResult
  case class NotIngested(reason: String) extends IngestResult

  private val excessiveFieldsError = "Cannot ingest values with more than %d primitive fields. This limitiation may be lifted in a future release. Thank you for your patience.".format(maxFields)

  trait BatchIngest {
    def apply(data: ByteChunk, parseDirectives: Set[ParseDirective], jobId: JobId, sync: Boolean): Future[IngestResult]
  }

  case class JSONParseState(parser: AsyncParser, apiKey: APIKey, path: Path, authorities: Authorities, jobId: Option[JobId], ingested: Int, errors: Seq[(Int, String)]) {
    def update(newParser: AsyncParser, newIngested: Int, newErrors: Seq[(Int, String)] = Seq.empty) =
      this.copy(parser = newParser, ingested = this.ingested + newIngested, errors = this.errors ++ newErrors)
  }

  def ingestJSONChunk(state: JSONParseState, stream: StreamT[Future, ByteBuffer]): Future[JSONParseState] = stream.uncons.flatMap {
    case Some((head, rest)) =>
      import state._
      val (result, updatedParser) = parser(Some(head))
      val toIngest = result.values.takeWhile { jv => jv.flattenWithPath.size <= maxFields }
      if (toIngest.size == result.values.size) {
        ingest(apiKey, path, authorities, result.values, jobId) flatMap { _ =>
          ingestJSONChunk(state.update(updatedParser, result.values.size, result.errors.map { pe => (pe.line, pe.msg) }), rest)
        }
      } else {
        ingest(apiKey, path, authorities, toIngest, jobId) map { _ =>
          state.update(updatedParser, toIngest.size, Seq((-1, excessiveFieldsError)))
        }
      }

    case None => Promise.successful {
      val (finalResult, finalParser) = state.parser(None)
      state.copy(parser = finalParser)
    }
  }

  class JSONBatchIngest(apiKey: APIKey, path: Path, authorities: Authorities) extends BatchIngest {
    def runParse(data: ByteChunk, sync: Boolean, jobId: JobId): Future[IngestResult] = {
      val parseFuture = ingestJSONChunk(JSONParseState(AsyncParser(), apiKey, path, authorities, Some(jobId), 0, Vector.empty), data match {
        case Left(buffer) => buffer :: StreamT.empty[Future, ByteBuffer]
        case Right(stream) => stream
      })

      if (sync) {
        parseFuture.map {
            case JSONParseState(_, _, _, _, _, ingested, errors) =>
            BatchSyncResult(ingested + errors.size, ingested, Vector(errors: _*))
        }
      } else {
        Promise.successful(AsyncSuccess)
      }
    }

    def apply(data: ByteChunk, parseDirectives: Set[ParseDirective], jobId: JobId, sync: Boolean): Future[IngestResult] =
      runParse(data, sync, jobId)
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

    /**
     * Normalize headers by turning them into `JPath`s. Normally, a field will
     * be mapped to a `JPath` simply by wrapping it in a `JPathField`. However,
     * in the case of duplicate headers, we turn that field into an array. So,
     * the header a,a,a will create objects of the form `{a:[_, _, _]}`.
     */
    def normalizeHeaders(headers: Array[String]): Array[JPath] = {
      val positions = headers.zipWithIndex.foldLeft(Map.empty[String, List[Int]]) {
        case (hdrs, (h, i)) =>
          val pos = i :: hdrs.getOrElse(h, Nil)
            hdrs + (h -> pos)
      }

      positions.toList.flatMap {
        case (h, Nil) =>
          Nil
        case (h, pos :: Nil) =>
          (pos -> JPath(JPathField(h))) :: Nil
        case (h, ps) =>
          ps.reverse.zipWithIndex map { case (pos, i) =>
            (pos -> JPath(JPathField(h), JPathIndex(i)))
          }
      }.sortBy(_._1).map(_._2).toArray
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
          readBatches(normalizeHeaders(header), reader, 0, 0, Vector())
        }
      }
    }

    def apply(data: ByteChunk, parseDirectives: Set[ParseDirective], jobId: JobId, sync: Boolean): Future[IngestResult] = {
      readerBuilder(parseDirectives) map { f =>
        if (sync) {
          // must not return until everything is persisted to kafka central
          val sink = new PipedOutputStream
          val src = new PipedInputStream
          sink.connect(src)
          val writeFuture = writeChunkStream(Channels.newChannel(sink), data)
          val readFuture = ingestSync(f(new BufferedReader(new InputStreamReader(src, "UTF-8"))), jobId)
          M.apply2(writeFuture, readFuture) { (written, result) => result }
        } else {
          for ((file, size) <- writeToFile(data)) yield {
            // spin off a future, but don't bother flatmapping through it since we
            // can return immediately
            ingestSync(f(new InputStreamReader(new FileInputStream(file), "UTF-8")), jobId)
            AsyncSuccess
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

  final private def writeChannel(chan: WritableByteChannel, stream: StreamT[Future, ByteBuffer], written: Long): Future[Long] = {
    stream.uncons flatMap {
      case Some((buf, tail)) =>
        // This is safe since the stream is coming directly from BE
        val safeBuf = buf.duplicate.rewind.asInstanceOf[ByteBuffer]
        //logger.trace("Writing buffer %s, remain: %d: %s".format(safeBuf.hashCode, safeBuf.remaining, safeBuf))
        try {
          val written0 = chan.write(safeBuf)
          writeChannel(chan, tail, written + written0)
        } catch {
          case t => logger.error("Failure on ByteBuffer read of %s (%d remaining)".format(safeBuf, safeBuf.remaining)); throw t
        }

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
    if (data.length > 0) {
      val eventInstance = Ingest(apiKey, path, Some(authorities), data, jobId, clock.instant())
      logger.trace("Saving event: " + eventInstance)
      eventStore.save(eventInstance, ingestTimeout)
    } else {
      Future {
        logger.warn("Unable to ingest empty set of values for %s at %s".format(apiKey, path.toString))
        PrecogUnit
      }
    }
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
    def array(unsafeBuffer: ByteBuffer): Array[Byte] = {
      // This ByteBuffer is coming straight from BE, so it's OK to dup/rewind
      val buffer = unsafeBuffer.duplicate.rewind.asInstanceOf[ByteBuffer]
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
    ingestJSONChunk(JSONParseState(AsyncParser(), apiKey, path, authorities, None, 0, Vector.empty), data match {
      case Left(buffer) => buffer :: StreamT.empty[Future, ByteBuffer]
      case Right(stream) => stream
    }).map {
      case JSONParseState(_, _, _, _, _, ingested, errors) =>
        StreamingSyncResult(ingested, errors.headOption.map(_._2))
    }
  }

  val service: HttpRequest[ByteChunk] => Validation[NotServed, (APIKey, Path) => Future[HttpResponse[JValue]]] = (request: HttpRequest[ByteChunk]) => {
    logger.debug("Got request in ingest handler: " + request)
    Success { (apiKey: APIKey, path: Path) => {
      val timestamp = clock.now()
      def createJob: Future[String \/ JobId] = jobManager.createJob(apiKey, "ingest-" + path, "ingest", None, Some(timestamp)).map(_.id).run.recover {
        case t: Throwable => -\/(Option(t.getMessage).getOrElse(t.getClass.toString))
      }

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
            val batchMode = request.parameters.get('mode).exists(_ equalsIgnoreCase "batch") ||
                            request.parameters.get('sync).exists(_ equalsIgnoreCase "sync")

            if (batchMode) {
              for {
                batchId <- createJob
                ingestResult <- batchId match {
                  case err: -\/[_] => Promise.successful(err)

                  case \/-(bid) =>
                    val sync = request.parameters.get('receipt) match {
                      case Some(v) => v equalsIgnoreCase "true"
                      case None => request.parameters.get('sync) forall (_ equalsIgnoreCase "sync")
                    }

                    ingestBatch(apiKey, path, authorities, content, parseDirectives, bid, sync).recover {
                      case t: Throwable =>
                        logger.error("Failure on ingest", t)
                        NotIngested(Option(t.getMessage).getOrElse(t.getClass.toString))
                    }.map { result => \/-((bid, result)) }
                }
              } yield {
                ingestResult.fold(
                  { message =>
                    logger.error("Internal error during ingest; got bad response from the jobs server: " + message)
                    HttpResponse[JValue](InternalServerError, content = Some(JString("An error occurred creating a batch ingest job: " + message)))},
                  {
                    case (bid, result) =>
                      def jobMessage(channel: String, message: String) =
                        jobManager.addMessage(bid, channel, JString(message))

                      result match {
                        case NotIngested(reason) =>
                          val message = "Ingest failed to %s with %s with reason: %s ".format(path, apiKey, reason)
                          logger.warn(message)
                          jobMessage(JobManager.channels.Warning, message)
                          HttpResponse[JValue](BadRequest, content = Some(JString(reason)))

                        case AsyncSuccess =>
                          val message = "Async ingest succeeded to %s with %s, batchId %s".format(path, apiKey, bid)
                          logger.info(message)
                          jobMessage(JobManager.channels.Info, message)
                          HttpResponse[JValue](Accepted, content = Some(JObject(JField("ingestId", JString(bid)) :: Nil)))

                        case BatchSyncResult(total, ingested, errors) =>
                          val failed = errors.size
                          val responseContent = JObject(
                            JField("total", JNum(total)),
                            JField("ingested", JNum(ingested)),
                            JField("failed", JNum(failed)),
                            JField("skipped", JNum(total - ingested - failed)),
                            JField("errors", JArray(errors map { case (line, msg) => JObject(JField("line", JNum(line)) :: JField("reason", JString(msg)) :: Nil) }: _*)),
                            JField("ingestId", JString(bid))
                          )

                          logger.info("Batch sync ingest succeeded to %s with %s. Result: %s".format(path, apiKey, responseContent.renderPretty))
                          jobManager.addMessage(bid, JobManager.channels.Info, responseContent)

                          if (ingested == 0 && total > 0) {
                            HttpResponse[JValue](BadRequest, content = Some(responseContent))
                          } else {
                            HttpResponse[JValue](OK, content = Some(responseContent))
                          }
                      }
                  }
                )
              }
            } else {
              ingestStreaming(apiKey, path, authorities, content, parseDirectives).map {
                case NotIngested(reason) =>
                    logger.warn("Streaming sync ingest failed to %s with %s with reason: %s ".format(path, apiKey, reason))
                    HttpResponse[JValue](BadRequest, content = Some(JString(reason)))

                case StreamingSyncResult(ingested, error) =>
                  val responseContent = JObject(JField("ingested", JNum(ingested)), JField("errors", JArray(error.map(JString(_)).toList)))
                  val responseCode = if (error.isDefined) { if (ingested == 0) BadRequest else RetryWith } else OK
                  logger.info("Streaming sync ingest succeeded to %s with %s. Result: %s".format(path, apiKey, responseContent.renderPretty))
                  if (ingested == 0 && !error.isDefined) // The following message is searched for by monit
                    logger.info("No ingested data and no errors to %s with %s. Headers: %s. Content: %s".format(path, apiKey,
                      request.headers, content.fold(_.toString(), _.map(x => x.toString()).toStream.apply().toList)))
                  HttpResponse(responseCode, content = Some(responseContent))
              }
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
