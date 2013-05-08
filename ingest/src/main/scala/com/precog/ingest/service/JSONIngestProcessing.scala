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
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._

sealed trait JSONRecordStyle
case object JSONValueStyle extends JSONRecordStyle
case object JSONStreamStyle extends JSONRecordStyle

final class JSONIngestProcessing(apiKey: APIKey, path: Path, authorities: Authorities, recordStyle: JSONRecordStyle, maxFields: Int, storage: IngestStore)(implicit M: Monad[Future]) extends IngestProcessing with Logging {

  def forRequest(request: HttpRequest[_]): ValidationNel[String, IngestProcessor] = {
    Success(new IngestProcessor)
  }

  case class IngestReport(ingested: Int, errors: Seq[(Int, String)])
  object IngestReport {
    val Empty = IngestReport(0, Vector())
  }

  case class JSONParseState(parser: AsyncParser, report: IngestReport) {
    def update(newParser: AsyncParser, newIngested: Int, newErrors: Seq[(Int, String)] = Seq.empty) =
      JSONParseState(newParser, IngestReport(report.ingested + newIngested, report.errors ++ newErrors))
  }

  object JSONParseState {
    def empty(stopOnFirstError: Boolean) = JSONParseState(AsyncParser(stopOnFirstError), IngestReport.Empty)
  }

  final class IngestProcessor extends IngestProcessorLike {
    def ingestJSONChunk(errorHandling: ErrorHandling, storeMode: StoreMode, jobId: Option[JobId], stream: StreamT[Future, ByteBuffer]): Future[IngestReport] = {
      val overLargeErrorBase = "Cannot ingest values with more than %d primitive fields. This limitiation may be lifted in a future release. Thank you for your patience.".format(maxFields)

      @inline def expandArraysAtRoot(values: Seq[JValue]) = recordStyle match {
        case JSONValueStyle => 
          values flatMap {
            case JArray(elements) => elements
            case value => Seq(value)
          }

        case JSONStreamStyle => 
          values
      }

      def ingestAllOrNothing(state: JSONParseState, stream: StreamT[Future, ByteBuffer], streamRef: StreamRef): Future[IngestReport] = {
        def accumulate(state: JSONParseState, records: Vector[JValue], stream: StreamT[Future, ByteBuffer]): Future[IngestReport] = {
          stream.uncons.flatMap {
            case Some((head, rest)) =>
              val toParse = head.duplicate.rewind.asInstanceOf[ByteBuffer]
              val (parsed, updatedParser) = state.parser(More(toParse))
              val ingestSize = parsed.values.size

              val overLargeIdx = parsed.values.indexWhere(_.flattenWithPath.size > maxFields)
              val errors = parsed.errors.map(pe => (pe.line, pe.msg)) ++ 
                           (overLargeIdx >= 0).option(overLargeIdx + state.report.ingested -> overLargeErrorBase)

              if (errors.isEmpty) {
                accumulate(state.update(updatedParser, ingestSize), records ++ parsed.values, rest)
              } else {
                IngestReport(0, errors).point[Future]
              }

            case None =>
              val (parsed, finalParser) = state.parser(Done)

              val overLargeIdx = parsed.values.indexWhere(_.flattenWithPath.size > maxFields)
              val errors = parsed.errors.map(pe => (pe.line, pe.msg)) ++ 
                           (overLargeIdx >= 0).option(overLargeIdx + state.report.ingested -> overLargeErrorBase)

              if (errors.isEmpty) {
                val completedRecords = records ++ parsed.values
                storage.store(apiKey, path, authorities, completedRecords, jobId, streamRef.terminate) map {
                  _.fold(
                    storeFailure => IngestReport(0, (0, storeFailure.message) :: Nil),
                    _ => IngestReport(completedRecords.size, Nil)
                  )
                }
              } else {
                IngestReport(0, errors).point[Future]
              }
          }
        }

        accumulate(state, Vector.empty[JValue], stream)
      }

      def ingestUnbuffered(state: JSONParseState, stream: StreamT[Future, ByteBuffer], streamRef: StreamRef): Future[JSONParseState] = {
        stream.uncons.flatMap {
          case Some((head, rest)) =>
            // Dup and rewind to ensure we have something to parse
            val toParse = head.duplicate.rewind.asInstanceOf[ByteBuffer]
            val (parsed, updatedParser) = state.parser(More(toParse))

            rest.isEmpty flatMap {
              case false => ingestBlock(parsed, updatedParser, state, streamRef) { ingestUnbuffered(_, rest, streamRef) }
              case true  => ingestFinalBlock(parsed, updatedParser, state, streamRef)
            }

          case None =>
            val (parsed, finalParser) = state.parser(Done)
            ingestFinalBlock(parsed, finalParser, state, streamRef)
        }
      }

      def ingestFinalBlock(parsed: AsyncParse, updatedParser: AsyncParser, state: JSONParseState, streamRef: StreamRef) = {
        ingestBlock(parsed, updatedParser, state, streamRef.terminate) { (_: JSONParseState).point[Future] }
      }

      def ingestBlock(parsed: AsyncParse, updatedParser: AsyncParser, state: JSONParseState, streamRef: StreamRef)(continue: => JSONParseState => Future[JSONParseState]): Future[JSONParseState] = {
        (errorHandling: @unchecked) match {
          case IngestAllPossible =>
            val (toIngest, overLarge) = expandArraysAtRoot(parsed.values) partition { jv => jv.flattenWithPath.size <= maxFields }
            val ingestSize = toIngest.size

            storage.store(apiKey, path, authorities, toIngest, jobId, streamRef) flatMap { 
              _.fold(
                storeFailure => sys.error("Do something useful with %s" format storeFailure.message),
                _ => {
                  val overLargeError = (-1, (overLargeErrorBase + " (%d records affected)").format(overLarge.size))
                  continue(state.update(updatedParser, ingestSize, parsed.errors.map(pe => (pe.line, pe.msg)) :+ overLargeError))
                }
              )
            }

          case StopOnFirstError =>
            val (toIngest, overLarge) = expandArraysAtRoot(parsed.values) span { jv => jv.flattenWithPath.size <= maxFields }
            val ingestSize = toIngest.size

            if (overLarge.isEmpty && parsed.errors.isEmpty) {
              storage.store(apiKey, path, authorities, toIngest, jobId, streamRef) flatMap { 
                _.fold(
                  storeFailure => sys.error("Do something useful with %s" format storeFailure.message),
                  _ => continue(state.update(updatedParser, ingestSize, Nil))
                )
              }
            } else {
              storage.store(apiKey, path, authorities, toIngest, jobId, streamRef.terminate) map {
                _.fold(
                  storeFailure => sys.error("Do something useful with%s" format storeFailure.message),
                  _ => {
                    val errors = if (overLarge.nonEmpty) List((state.report.ingested + toIngest.size, overLargeErrorBase))
                                 else parsed.errors.map(pe => (pe.line, pe.msg))

                    state.update(updatedParser, ingestSize, errors)
                  }
                )
              }
            } 
        }
      }

      errorHandling match {
        case StopOnFirstError => 
          ingestUnbuffered(JSONParseState.empty(true), stream, storeMode.createStreamRef(false)) map { _.report }
        case IngestAllPossible => 
          ingestUnbuffered(JSONParseState.empty(false), stream, storeMode.createStreamRef(false)) map { _.report }
        case AllOrNothing => 
          ingestAllOrNothing(JSONParseState.empty(true), stream, storeMode.createStreamRef(false))
      }
    }

    def ingest(durability: Durability, errorHandling: ErrorHandling, storeMode: StoreMode, data: ByteChunk): Future[IngestResult] = {
      val dataStream = data match {
        case Left(buffer) => buffer :: StreamT.empty[Future, ByteBuffer]
        case Right(stream) => stream
      }

      durability match {
        case LocalDurability =>
          ingestJSONChunk(errorHandling, storeMode, None, dataStream) map {
            case IngestReport(ingested, errors) =>
              errorHandling match {
                case StopOnFirstError | AllOrNothing =>
                  StreamingResult(ingested, errors.headOption.map(_._2))

                case IngestAllPossible =>
                  BatchResult(ingested + errors.size, ingested, Vector(errors: _*))
              }
          }

        case GlobalDurability(jobId) =>
          ingestJSONChunk(errorHandling, storeMode, Some(jobId), dataStream) map {
            case IngestReport(ingested, errors) =>
              BatchResult(ingested + errors.size, ingested, Vector(errors: _*))
          }
      } 
    }
  }
}
