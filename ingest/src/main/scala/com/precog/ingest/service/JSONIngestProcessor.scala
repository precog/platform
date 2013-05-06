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
import scala.collection.mutable.ArrayBuffer

sealed trait JsonRecordStyle
case object JsonValueStyle extends JsonRecordStyle
case object JsonStreamStyle extends JsonRecordStyle

final class JSONIngestProcessor(apiKey: APIKey, path: Path, authorities: Authorities, recordStyle: JsonRecordStyle, maxFields: Int, ingest: IngestStore)(implicit M: Monad[Future], val executor: ExecutionContext)
 extends IngestProcessor with Logging {

  case class JSONParseState(parser: AsyncParser, jobId: Option[JobId], ingested: Int, errors: Seq[(Int, String)], total: Int) {
    def update(newParser: AsyncParser, newIngested: Int, newErrors: Seq[(Int, String)] = Seq.empty) =
      this.copy(parser = newParser, ingested = this.ingested + newIngested, errors = this.errors ++ newErrors, total = this.total + newIngested + newErrors.size)
  }

  def ingestJSONChunk(state: JSONParseState, stream: StreamT[Future, ByteBuffer], stopOnFirstError: Boolean)(implicit M: Monad[Future], executor: ExecutionContext): Future[JSONParseState] =
    stream.uncons.flatMap {
      case Some((head, rest)) =>
        // Dup and rewind to ensure we have something to parse
        val toParse = head.duplicate.rewind.asInstanceOf[ByteBuffer]
        logger.trace("Async parse on " + toParse)
        val (parsed, updatedParser) = state.parser(Some(toParse))
        ingestBlock(state, parsed, updatedParser, stopOnFirstError) { ingestJSONChunk(_: JSONParseState, rest, stopOnFirstError) }

      case None => 
        val (finalResult, finalParser) = state.parser(None)
        ingestBlock(state, finalResult, finalParser, stopOnFirstError) { Promise successful _ }
    }

  def ingestBlock(state: JSONParseState, parsed: AsyncParse, parser: AsyncParser, stopOnFirstError: Boolean)(continue: JSONParseState => Future[JSONParseState])(implicit M: Monad[Future]): Future[JSONParseState] = {
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
      val (toIngest, oversizeIdxs, total) = if (stopOnFirstError) {
        val (prefix, suffix) = records.span(jv => jv.flattenWithPath.size <= maxFields)
        if (suffix.nonEmpty) (prefix, Seq((prefix.size, suffix.head.flattenWithPath.size)), prefix.size)
        else (prefix, Nil, prefix.size)
      } else {
        records.foldLeft((ArrayBuffer.empty[JValue], ArrayBuffer.empty[(Int, Int)], 0)) { case ((toIngest, errorIdx, idx), jv) => 
          val eventSize = jv.flattenWithPath.size
          if (eventSize <= maxFields) (toIngest += jv, errorIdx, idx + 1)
          else (toIngest, errorIdx += (state.total + idx -> eventSize), idx + 1)
        }
      }

      if (toIngest.size == records.size) {
        ingest.store(apiKey, path, authorities, records, state.jobId) flatMap { _ =>
          continue(state.update(parser, records.size, parsed.errors.map(pe => pe.line -> pe.msg)))
        }
      } else {
        val sizeErrs = oversizeIdxs map { case (idx, size) => 
          (state.total + idx, "Value has size %d; cannot ingest values with more than %d primitive fields. This limitiation may be lifted in a future release. Thank you for your patience.".format(size, maxFields)) 
        }

        ingest.store(apiKey, path, authorities, toIngest, state.jobId) flatMap { _ =>
          if (stopOnFirstError) {
            M point state.update(parser, toIngest.size, sizeErrs ++ parsed.errors.map(pe => pe.line -> pe.msg))
          } else {
            continue(state.update(parser, toIngest.size, sizeErrs ++ parsed.errors.map(pe => pe.line -> pe.msg)))
          }
        }
      }
    } else {
      logger.debug("Async parse of chunk resulted in zero values, %d errors".format(parsed.errors.size))
      continue(state.update(parser, 0, parsed.errors.map { pe => (pe.line, pe.msg) }))
    }
  }

  def processBatch(data: ByteChunk, parseDirectives: Set[ParseDirective], jobId: JobId, sync: Boolean): Future[BatchIngestResult] = {
    val dataStream = data match {
      case Left(buffer) => buffer :: StreamT.empty[Future, ByteBuffer]
      case Right(stream) => stream
    }

    val parseFuture = ingestJSONChunk(JSONParseState(AsyncParser(false), Some(jobId), 0, Vector.empty, 0), dataStream, false)

    if (sync) {
      parseFuture.map {
        case JSONParseState(_, _, ingested, errors, total) =>
          BatchSyncResult(total, ingested, Vector(errors: _*))
      }
    } else {
      Promise.successful(AsyncSuccess)
    }
  }

  def processStream(data: ByteChunk, parseDirectives: Set[ParseDirective]): Future[StreamingIngestResult] = {
    val dataStream = data match {
      case Left(buffer) => buffer :: StreamT.empty[Future, ByteBuffer]
      case Right(stream) => stream
    }

    ingestJSONChunk(JSONParseState(AsyncParser(true), None, 0, Vector.empty, 0), dataStream, true).map {
      case JSONParseState(_, _, ingested, errors, total) =>
        StreamingSyncResult(ingested, errors.headOption.map(_._2))
    }.recover {
      case t: Throwable =>
        logger.error("Failure on ingest", t)
        NotIngested(Option(t.getMessage).getOrElse(t.getClass.toString))
    }
  }
}
