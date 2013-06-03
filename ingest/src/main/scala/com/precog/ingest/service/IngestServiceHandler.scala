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
import akka.util.Timeout

import blueeyes.bkka._
import blueeyes.core.data.ByteChunk
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.HttpHeaders._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.Extractor
import blueeyes.util.Clock

import com.google.common.base.Charsets

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.{ Executor, RejectedExecutionException }

import com.weiglewilczek.slf4s.Logging

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

sealed trait ParseDirective {
  def toMap: Map[String, String] // escape hatch for interacting with other systems
}

case class CSVDelimiter(delimiter: String) extends ParseDirective { val toMap = Map("csv:delimiter" -> delimiter) }
case class CSVQuote(quote: String) extends ParseDirective { val toMap = Map("csv:quote" -> quote) }
case class CSVSeparator(separator: String) extends ParseDirective { val toMap = Map("csv:separator" -> separator) }
case class CSVEscape(escape: String) extends ParseDirective { val toMap = Map("csv:escape" -> escape) }
case class MimeDirective(mimeType: MimeType) extends ParseDirective { val toMap = Map("content-type" -> mimeType.toString) }

sealed trait BatchIngestResult
sealed trait StreamingIngestResult

case object AsyncSuccess extends BatchIngestResult
case class BatchSyncResult(total: Int, ingested: Int, errors: Vector[(Int, String)]) extends BatchIngestResult

case class StreamingSyncResult(ingested: Int, error: Option[String]) extends StreamingIngestResult
case class NotIngested(reason: String) extends BatchIngestResult with StreamingIngestResult

sealed trait IngestStore {
  def store(apiKey: APIKey, path: Path, authorities: Authorities, data: Seq[JValue], jobId: Option[JobId]): Future[PrecogUnit]
}

class IngestServiceHandler(
  permissionsFinder: PermissionsFinder[Future],
  jobManager: JobManager[Response],
  clock: Clock,
  eventStore: EventStore[Future],
  ingestTimeout: Timeout,
  batchSize: Int,
  maxFields: Int,
  tmpdir: File)(implicit M: Monad[Future], executor: ExecutionContext)
    extends CustomHttpService[ByteChunk, (APIKey, Path) => Future[HttpResponse[JValue]]]
    with IngestStore
    with Logging { ingestStore =>

  private[this] val processorSelector = new IngestProcessorSelection(maxFields, batchSize, tmpdir, ingestStore)
  import processorSelector._

  def store(apiKey: APIKey, path: Path, authorities: Authorities, data: Seq[JValue], jobId: Option[JobId]): Future[PrecogUnit] = {
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

  def chooseProcessor(apiKey: APIKey, path: Path, authorities: Authorities, data: ByteChunk, parseDirectives: Set[ParseDirective]): Future[Option[IngestProcessor]] = {
    def array(unsafeBuffer: ByteBuffer): Array[Byte] = {
      // This ByteBuffer is coming straight from BE, so it's OK to dup/rewind
      val buffer = unsafeBuffer.duplicate.rewind.asInstanceOf[ByteBuffer]
      val target = new Array[Byte](buffer.remaining)
      buffer.get(target)
      buffer.flip()
      target
    }

    val selectors = ingestSelectors(apiKey, path, authorities)

    data match {
      case Left(buf) =>
        M.point(selectIngestProcessor(selectors, array(buf), parseDirectives))

      case Right(stream) =>
        stream.uncons map {
          _ flatMap { case (buf, _) => selectIngestProcessor(selectors, array(buf), parseDirectives) }
        }
    }
  }

  def ingestBatch(apiKey: APIKey, path: Path, authorities: Authorities, data: ByteChunk, parseDirectives: Set[ParseDirective], batchJob: JobId, sync: Boolean): Future[BatchIngestResult] = {
    chooseProcessor(apiKey, path, authorities, data, parseDirectives) flatMap {
      case Some(processor) => processor.processBatch(data, parseDirectives, batchJob, sync)
      case None => M.point(NotIngested("Could not successfully determine a data type for your batch ingest. Please consider setting the Content-Type header."))
    }
  }

  def ingestStreaming(apiKey: APIKey, path: Path, authorities: Authorities, data: ByteChunk, parseDirectives: Set[ParseDirective]): Future[StreamingIngestResult] = {
    chooseProcessor(apiKey, path, authorities, data, parseDirectives) flatMap {
      case Some(processor) => processor.processStream(data, parseDirectives)
      case None => M.point(NotIngested("Could not successfully determine a data type for your streaming ingest. Please consider setting the Content-Type header."))
    }
  }

  val service: HttpRequest[ByteChunk] => Validation[NotServed, (APIKey, Path) => Future[HttpResponse[JValue]]] = (request: HttpRequest[ByteChunk]) => {
    logger.debug("Got request in ingest handler: " + request)
    Success { (apiKey: APIKey, path: Path) => {
      val timestamp = clock.now()
      def createJob: EitherT[Future, String, JobId] = jobManager.createJob(apiKey, "ingest-" + path, "ingest", None, Some(timestamp)).map(_.id)

      //.run.recover {
      //  case t: Throwable => -\/(Option(t.getMessage).getOrElse(t.getClass.toString))
      //}

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
              (for {
                jobId <- createJob.leftMap { message =>
                  logger.error("Internal error during ingest; got bad response from the jobs server: " + message)
                  HttpResponse[JValue](InternalServerError, content = Some(JString("An error occurred creating a batch ingest job: " + message)))
                }
                ingestResult <- {
                  val sync = request.parameters.get('receipt) match {
                    case Some(v) => v equalsIgnoreCase "true"
                    case None => request.parameters.get('sync) forall (_ equalsIgnoreCase "sync")
                  }

                  EitherT.right(ingestBatch(apiKey, path, authorities, content, parseDirectives, jobId, sync).recover {
                    case t: Throwable =>
                      logger.error("Failure on ingest", t)
                      NotIngested(Option(t.getMessage).getOrElse(t.getClass.toString))
                  })
                }
              } yield {
                def jobMessage(channel: String, message: String) =
                  jobManager.addMessage(jobId, channel, JString(message))

                ingestResult match {
                  case NotIngested(reason) =>
                    val message = "Ingest failed to %s with %s with reason: %s ".format(path, apiKey, reason)
                    logger.warn(message)
                    jobMessage(JobManager.channels.Warning, message)
                    HttpResponse[JValue](BadRequest, content = Some(JString(reason)))

                  case AsyncSuccess =>
                    val message = "Async ingest succeeded to %s with %s, jobId %s".format(path, apiKey, jobId)
                    logger.info(message)
                    jobMessage(JobManager.channels.Info, message)
                    HttpResponse[JValue](Accepted, content = Some(JObject(JField("ingestId", JString(jobId)) :: Nil)))

                  case BatchSyncResult(total, ingested, errors) =>
                    val failed = errors.size
                    val responseContent = JObject(
                      JField("total", JNum(total)),
                      JField("ingested", JNum(ingested)),
                      JField("failed", JNum(failed)),
                      JField("skipped", JNum(total - ingested - failed)),
                      JField("errors", JArray(errors map { case (line, msg) => JObject(JField("line", JNum(line)) :: JField("reason", JString(msg)) :: Nil) }: _*)),
                      JField("ingestId", JString(jobId))
                    )

                    logger.info("Batch sync ingest succeeded to %s with %s. Result: %s".format(path, apiKey, responseContent.renderPretty))
                    jobManager.addMessage(jobId, JobManager.channels.Info, responseContent)

                    if (ingested == 0 && total > 0) {
                      HttpResponse[JValue](BadRequest, content = Some(responseContent))
                    } else {
                      HttpResponse[JValue](OK, content = Some(responseContent))
                    }
                }
              }).valueOr { x => x }
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

  val metadata = {
    import MimeTypes._
    import Metadata._
    and(
      about(
        or(requestHeader(`Content-Type`(application/json)), requestHeader(`Content-Type`(text/csv))),
        description("The content type of the ingested data should be specified. The service will attempt to infer structure if no content type is specified, but this may yield degraded or incorrect results under some circumstances.")
      ),
      description("""This service can be used to store one or more records, supplied as either whitespace-delimited JSON or CSV.""")
    )
  }
}
