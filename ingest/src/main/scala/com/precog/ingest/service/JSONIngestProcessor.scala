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

import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.core.data.ByteChunk
import blueeyes.json.AsyncParser

import com.precog.common.Path
import com.precog.common.ingest._
import com.precog.common.jobs.JobId
import com.precog.common.security.{APIKey, Authorities}

import com.weiglewilczek.slf4s.Logging

import java.nio.ByteBuffer

import scalaz._

final class JSONIngestProcessor(apiKey: APIKey, path: Path, authorities: Authorities, maxFields: Int, ingest: IngestStore)(implicit M: Monad[Future], val executor: ExecutionContext)
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
        val (result, updatedParser) = parser(Some(toParse))
        if (result.values.size > 0) {
          val toIngest = result.values.takeWhile { jv => jv.flattenWithPath.size <= maxFields }
          if (toIngest.size == result.values.size) {
            ingest.store(apiKey, path, authorities, result.values, jobId) flatMap { _ =>
              ingestJSONChunk(state.update(updatedParser, result.values.size, result.errors.map { pe => (pe.line, pe.msg) }), rest)
            }
          } else {
            ingest.store(apiKey, path, authorities, toIngest, jobId) map { _ =>
              state.update(updatedParser, toIngest.size, Seq((-1, "Cannot ingest values with more than %d primitive fields. This limitiation may be lifted in a future release. Thank you for your patience.".format(maxFields))))
            }
          }
        } else {
          logger.warn("Async parse of chunk resulted in zero values, %d errors".format(result.errors.size))
          ingestJSONChunk(state.update(updatedParser, 0, result.errors.map { pe => (pe.line, pe.msg) }), rest)
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
