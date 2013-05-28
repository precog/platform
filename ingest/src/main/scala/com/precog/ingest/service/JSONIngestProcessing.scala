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
import com.precog.common.security.{APIKey, Authorities, WriteMode}

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
    def update(newParser: AsyncParser, newIngested: Int, newErrors: Seq[(Int, String)] = Seq.empty) = {
      JSONParseState(newParser, IngestReport(report.ingested + newIngested, report.errors ++ newErrors))
    }
  }

  object JSONParseState {
    def empty(stopOnFirstError: Boolean) = JSONParseState(AsyncParser.stream(), IngestReport.Empty)
  }

  final class IngestProcessor extends IngestProcessorLike {
    def ingestJSONChunk(errorHandling: ErrorHandling, storeMode: WriteMode, jobId: Option[JobId], stream: StreamT[Future, Array[Byte]]): Future[IngestReport] = {
      val overLargeMsg = "Cannot ingest values with more than %d primitive fields. This limitiation may be lifted in a future release. Thank you for your patience.".format(maxFields)

      @inline def expandArraysAtRoot(values: Seq[JValue]) = recordStyle match {
        case JSONValueStyle => 
          values flatMap {
            case JArray(elements) => elements
            case value => Seq(value)
          }

        case JSONStreamStyle => 
          values
      }

      def ingestAllOrNothing(state: JSONParseState, stream: StreamT[Future, Array[Byte]], streamRef: StreamRef): Future[IngestReport] = {
        def accumulate(state: JSONParseState, records: Vector[JValue], stream: StreamT[Future, Array[Byte]]): Future[IngestReport] = {
          stream.uncons.flatMap {
            case Some((bytes, rest)) =>
              val (parsed, updatedParser) = state.parser(More(ByteBuffer.wrap(bytes)))
              val ingestSize = parsed.values.size

              val overLargeIdx = parsed.values.indexWhere(_.flattenWithPath.size > maxFields)
              val errors = parsed.errors.map(pe => (pe.line, pe.msg)) ++ 
                           (overLargeIdx >= 0).option(overLargeIdx + state.report.ingested -> overLargeMsg)

              if (errors.isEmpty) {
                accumulate(state.update(updatedParser, ingestSize), records ++ parsed.values, rest)
              } else {
                IngestReport(0, errors).point[Future]
              }

            case None =>
              val (parsed, finalParser) = state.parser(Done)

              val overLargeIdx = parsed.values.indexWhere(_.flattenWithPath.size > maxFields)
              val errors = parsed.errors.map(pe => (pe.line, pe.msg)) ++ 
                           (overLargeIdx >= 0).option(overLargeIdx + state.report.ingested -> overLargeMsg)

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

      def ingestUnbuffered(state: JSONParseState, stream: StreamT[Future, Array[Byte]], streamRef: StreamRef): Future[JSONParseState] = {
        stream.uncons.flatMap {
          case Some((bytes, rest)) =>
            // Dup and rewind to ensure we have something to parse
            val (parsed, updatedParser) = state.parser(More(ByteBuffer.wrap(bytes)))

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

      def partitionIndexed[A](as: Seq[A])(f: A => Boolean): (Seq[A], Seq[Int]) = {
        var ok: Vector[A] = Vector()
        var ko: Vector[Int] = Vector()
        var i = 0
        as foreach { a =>
          if (f(a)) ko = ko :+ i else ok = ok :+ a
          i += 1
        }
        
        (ok, ko)
      }

      def ingestBlock(parsed: AsyncParse, updatedParser: AsyncParser, state: JSONParseState, streamRef: StreamRef)(continue: => JSONParseState => Future[JSONParseState]): Future[JSONParseState] = {
        (errorHandling: @unchecked) match {
          case IngestAllPossible =>
            val (toIngest, overLarge) = partitionIndexed(expandArraysAtRoot(parsed.values)) { _.flattenWithPath.size > maxFields }
            val ingestSize = toIngest.size

            storage.store(apiKey, path, authorities, toIngest, jobId, streamRef) flatMap { 
              _.fold(
                storeFailure => sys.error("Do something useful with %s" format storeFailure.message),
                _ => {
                  val errors = parsed.errors.map(pe => (pe.line, pe.msg)) ++ overLarge.map(i => (i, overLargeMsg))
                  continue(state.update(updatedParser, ingestSize, errors))
                }
              )
            }

          case StopOnFirstError =>
            val (toIngest, overLarge) = expandArraysAtRoot(parsed.values) span { _.flattenWithPath.size <= maxFields }
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
                    val errors = parsed.errors.map(pe => (pe.line, pe.msg)) ++ 
                                 (overLarge.nonEmpty).option(state.report.ingested + toIngest.size -> overLargeMsg)

                    state.update(updatedParser, ingestSize, errors)
                  }
                )
              }
            } 
        }
      }

      errorHandling match {
        case StopOnFirstError => 
          ingestUnbuffered(JSONParseState.empty(true), stream, StreamRef.forWriteMode(storeMode, false)) map { _.report }
        case IngestAllPossible => 
          ingestUnbuffered(JSONParseState.empty(false), stream, StreamRef.forWriteMode(storeMode, false)) map { _.report }
        case AllOrNothing => 
          ingestAllOrNothing(JSONParseState.empty(true), stream, StreamRef.forWriteMode(storeMode, false))
      }
    }

    def ingest(durability: Durability, errorHandling: ErrorHandling, storeMode: WriteMode, data: ByteChunk): Future[IngestResult] = {
      val dataStream = data.fold(_ :: StreamT.empty[Future, Array[Byte]], identity)

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
