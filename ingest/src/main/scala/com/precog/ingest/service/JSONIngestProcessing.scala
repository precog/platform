package com.precog.ingest
package service

import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.core.data.ByteChunk
import blueeyes.core.http.HttpRequest
import blueeyes.json._
import IngestProcessing._
import AsyncParser._

import com.precog.common.Path
import com.precog.common.ingest._
import com.precog.common.jobs.JobId
import com.precog.common.security.{APIKey, Authorities}

import com.weiglewilczek.slf4s.Logging

import java.nio.ByteBuffer

import scalaz._

sealed trait JSONRecordStyle
case object JSONValueStyle extends JSONRecordStyle
case object JSONStreamStyle extends JSONRecordStyle

final class JSONIngestProcessing(apiKey: APIKey, path: Path, authorities: Authorities, recordStyle: JSONRecordStyle, maxFields: Int, ingest: IngestStore)(implicit M: Monad[Future]) extends IngestProcessing with Logging {

  def forRequest(request: HttpRequest[_]): ValidationNel[String, IngestProcessor] = {
    Success(new IngestProcessor)
  }

  case class JSONParseState(parser: AsyncParser, ingested: Int, errors: Seq[(Int, String)]) {
    def update(newParser: AsyncParser, newIngested: Int, newErrors: Seq[(Int, String)] = Seq.empty) =
      this.copy(parser = newParser, ingested = this.ingested + newIngested, errors = this.errors ++ newErrors)
  }

  object JSONParseState {
    def empty(stopOnFirstError: Boolean) = JSONParseState(AsyncParser(stopOnFirstError), 0, Vector.empty)
  }

  def ingestJSONChunk(errorHandling: ErrorHandling, jobId: Option[JobId], stream: StreamT[Future, ByteBuffer]): Future[JSONParseState] = {
    def rec(state: JSONParseState, stream: StreamT[Future, ByteBuffer]): Future[JSONParseState] = {
      stream.uncons.flatMap {
        case Some((head, rest)) =>
          import state._
          // Dup and rewind to ensure we have something to parse
          val toParse = head.duplicate.rewind.asInstanceOf[ByteBuffer]
          logger.trace("Async parse on " + toParse)
          val (parsed, updatedParser) = parser(More(toParse))
          val records = recordStyle match {
            case JSONValueStyle => 
              parsed.values flatMap {
                case JArray(elements) => elements
                case value => Seq(value)
              }

            case JSONStreamStyle => 
              parsed.values
          }

          if (records.size > 0) {
            val toIngest = records.takeWhile { jv => jv.flattenWithPath.size <= maxFields }
            if (toIngest.size == records.size) {
              ingest.store(apiKey, path, authorities, records, jobId) flatMap { _ =>
                rec(state.update(updatedParser, records.size, parsed.errors.map { pe => (pe.line, pe.msg) }), rest)
              }
            } else {
              ingest.store(apiKey, path, authorities, toIngest, jobId) map { _ =>
                state.update(updatedParser, toIngest.size, Seq((-1, "Cannot ingest values with more than %d primitive fields. This limitiation may be lifted in a future release. Thank you for your patience.".format(maxFields))))
              }
            }
          } else {
            logger.warn("Async parse of chunk resulted in zero values, %d errors".format(parsed.errors.size))
            rec(state.update(updatedParser, 0, parsed.errors.map { pe => (pe.line, pe.msg) }), rest)
          }

        case None => M point {
          val (finalResult, finalParser) = state.parser(Done)
          state.copy(parser = finalParser)
        }
      }
    }

    val parseState = errorHandling match {
      case StopOnFirstError | AllOrNothing => JSONParseState.empty(true)
      case IngestAllPossible => JSONParseState.empty(false)
    }

    rec(parseState, stream)
  }

  final class IngestProcessor extends IngestProcessorLike {
    def ingest(durability: Durability, errorHandling: ErrorHandling, data: ByteChunk): Future[IngestResult] = {
      val dataStream = data match {
        case Left(buffer) => buffer :: StreamT.empty[Future, ByteBuffer]
        case Right(stream) => stream
      }

      durability match {
        case LocalDurability =>
          ingestJSONChunk(errorHandling, None, dataStream) map {
            case JSONParseState(_, ingested, errors) =>
              errorHandling match {
                case StopOnFirstError | AllOrNothing =>
                  StreamingResult(ingested, errors.headOption.map(_._2))

                case IngestAllPossible =>
                  BatchResult(ingested + errors.size, ingested, Vector(errors: _*))
              }
          }

        case GlobalDurability(jobId) =>
          ingestJSONChunk(errorHandling, Some(jobId), dataStream) map {
            case JSONParseState(_, ingested, errors) =>
              BatchResult(ingested + errors.size, ingested, Vector(errors: _*))
          }
      } 
    }
  }
}
