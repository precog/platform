package com.precog.ingest
package service

import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.core.data.ByteChunk
import blueeyes.json._
import blueeyes.json.AsyncParser

import com.precog.common.Path
import com.precog.common.ingest._
import com.precog.common.jobs.JobId
import com.precog.common.security.{APIKey, Authorities}

import com.weiglewilczek.slf4s.Logging

import java.nio.ByteBuffer

import scalaz._

sealed trait JsonRecordStyle
case object JsonValueStyle extends JsonRecordStyle
case object JsonStreamStyle extends JsonRecordStyle

final class JSONIngestProcessor(apiKey: APIKey, path: Path, authorities: Authorities, recordStyle: JsonRecordStyle, maxFields: Int, ingest: IngestStore)(implicit M: Monad[Future], val executor: ExecutionContext)
 extends IngestProcessor with Logging {

  case class JSONParseState(parser: AsyncParser, jobId: Option[JobId], ingested: Int, errors: Seq[(Int, String)]) {
    def update(newParser: AsyncParser, newIngested: Int, newErrors: Seq[(Int, String)] = Seq.empty) =
      this.copy(parser = newParser, ingested = this.ingested + newIngested, errors = this.errors ++ newErrors)
  }

  def ingestJSONChunk(state: JSONParseState, stream: StreamT[Future, ByteBuffer])(implicit M: Monad[Future], executor: ExecutionContext): Future[JSONParseState] =
    stream.uncons.flatMap {
      case Some((head, rest)) =>
        import state._
        // Dup and rewind to ensure we have something to parse
        val toParse = head.duplicate.rewind.asInstanceOf[ByteBuffer]
        logger.trace("Async parse on " + toParse)
        val (parsed, updatedParser) = parser(Some(toParse))
        val records = recordStyle match {
          case JsonValueStyle => 
            parsed.values flatMap {
              case JArray(elements) => elements
              case value => Seq(value)
            }

          case JsonStreamStyle => 
            parsed.values
        }

        if (records.size > 0) {
          val toIngest = records.takeWhile { jv => jv.flattenWithPath.size <= maxFields }
          if (toIngest.size == records.size) {
            ingest.store(apiKey, path, authorities, records, jobId) flatMap { _ =>
              ingestJSONChunk(state.update(updatedParser, records.size, parsed.errors.map { pe => (pe.line, pe.msg) }), rest)
            }
          } else {
            ingest.store(apiKey, path, authorities, toIngest, jobId) map { _ =>
              state.update(updatedParser, toIngest.size, Seq((-1, "Cannot ingest values with more than %d primitive fields. This limitiation may be lifted in a future release. Thank you for your patience.".format(maxFields))))
            }
          }
        } else {
          logger.warn("Async parse of chunk resulted in zero values, %d errors".format(parsed.errors.size))
          ingestJSONChunk(state.update(updatedParser, 0, parsed.errors.map { pe => (pe.line, pe.msg) }), rest)
        }

      case None => Promise.successful {
        val (finalResult, finalParser) = state.parser(None)
        state.copy(parser = finalParser)
      }
    }

  def processBatch(data: ByteChunk, parseDirectives: Set[ParseDirective], jobId: JobId, sync: Boolean): Future[BatchIngestResult] = {
    val parseFuture = ingestJSONChunk(JSONParseState(AsyncParser(), Some(jobId), 0, Vector.empty), data match {
      case Left(buffer) => buffer :: StreamT.empty[Future, ByteBuffer]
      case Right(stream) => stream
    })

    if (sync) {
      parseFuture.map {
        case JSONParseState(_, _, ingested, errors) =>
          BatchSyncResult(ingested + errors.size, ingested, Vector(errors: _*))
      }
    } else {
      Promise.successful(AsyncSuccess)
    }
  }

  def processStream(data: ByteChunk, parseDirectives: Set[ParseDirective]): Future[StreamingIngestResult] = {
    ingestJSONChunk(JSONParseState(AsyncParser(), None, 0, Vector.empty), data match {
      case Left(buffer) => buffer :: StreamT.empty[Future, ByteBuffer]
      case Right(stream) => stream
    }).map {
      case JSONParseState(_, _, ingested, errors) =>
        StreamingSyncResult(ingested, errors.headOption.map(_._2))
    }.recover {
      case t: Throwable =>
        logger.error("Failure on ingest", t)
        NotIngested(Option(t.getMessage).getOrElse(t.getClass.toString))
    }
  }
}
