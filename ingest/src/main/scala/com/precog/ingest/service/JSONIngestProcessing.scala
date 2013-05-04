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

sealed trait JSONRecordStyle
case object JSONValueStyle extends JSONRecordStyle
case object JSONStreamStyle extends JSONRecordStyle

final class JSONIngestProcessing(apiKey: APIKey, path: Path, authorities: Authorities, recordStyle: JSONRecordStyle, maxFields: Int, storage: IngestStore)(implicit M: Monad[Future]) extends IngestProcessing with Logging {

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

  final class IngestProcessor extends IngestProcessorLike {
    def ingestJSONChunk(errorHandling: ErrorHandling, storeMode: StoreMode, jobId: Option[JobId], stream: StreamT[Future, ByteBuffer]): Future[JSONParseState] = {
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

      def ingestAllOrNothing(state: JSONParseState, stream: StreamT[Future, ByteBuffer], streamRef: StreamRef): Future[JSONParseState] = {
        def accumulate(state: JSONParseState, records: Vector[JValue], stream: StreamT[Future, ByteBuffer]): Future[JSONParseState] = {
          stream.uncons.flatMap {
            case Some((head, rest)) =>
              val toParse = head.duplicate.rewind.asInstanceOf[ByteBuffer]
              val (parsed, updatedParser) = state.parser(More(toParse))
              val ingestSize = parsed.values.size
              val errors = if (parsed.errors.isEmpty && parsed.values.exists(_.flattenWithPath.size > maxFields)) List((state.ingested, overLargeErrorBase))
                           else parsed.errors.map(pe => (pe.line, pe.msg))

              if (errors.isEmpty) {
                accumulate(state.update(updatedParser, ingestSize), records ++ parsed.values, rest)
              } else {
                state.update(updatedParser, ingestSize, errors).point[Future]
              }

            case None =>
              val (parsed, finalParser) = state.parser(Done)
              val ingestSize = parsed.values.size
              val errors = if (parsed.errors.isEmpty && parsed.values.exists(_.flattenWithPath.size > maxFields)) List((state.ingested, overLargeErrorBase))
                           else parsed.errors.map(pe => (pe.line, pe.msg))

              if (errors.isEmpty) {
                storage.store(apiKey, path, authorities, records ++ parsed.values, jobId, streamRef.terminate) map { _ =>
                  sys.error("Do something useful with StoreFailure")
                  state.update(finalParser, ingestSize, Nil)
                }
              } else {
                state.update(finalParser, ingestSize, errors).point[Future]
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
            ingestBlock(parsed, updatedParser, state, streamRef) { ingestUnbuffered(_, rest, streamRef) }

          case None =>
            val (parsed, finalParser) = state.parser(Done)
            ingestBlock(parsed, finalParser, state, streamRef.terminate) { (_: JSONParseState).point[Future] }
        }
      }

      def ingestBlock(parsed: AsyncParse, updatedParser: AsyncParser, state: JSONParseState, streamRef: StreamRef)(continue: => JSONParseState => Future[JSONParseState]): Future[JSONParseState] = {
        (errorHandling: @unchecked) match {
          case IngestAllPossible =>
            val (toIngest, overLarge) = expandArraysAtRoot(parsed.values) partition { jv => jv.flattenWithPath.size <= maxFields }
            val ingestSize = toIngest.size

            storage.store(apiKey, path, authorities, toIngest, jobId, streamRef) flatMap { _ =>
              sys.error("Do something useful with StoreFailure")
              val overLargeError = (-1, (overLargeErrorBase + " (%d records affected)").format(overLarge.size))
              continue(state.update(updatedParser, ingestSize, parsed.errors.map(pe => (pe.line, pe.msg)) :+ overLargeError))
            }

          case StopOnFirstError =>
            val (toIngest, overLarge) = expandArraysAtRoot(parsed.values) span { jv => jv.flattenWithPath.size <= maxFields }
            val ingestSize = toIngest.size

            if (overLarge.isEmpty && parsed.errors.isEmpty) {
              storage.store(apiKey, path, authorities, toIngest, jobId, streamRef) flatMap { _ =>
                sys.error("Do something useful with StoreFailure")
                continue(state.update(updatedParser, ingestSize, Nil))
              }
            } else {
              storage.store(apiKey, path, authorities, toIngest, jobId, streamRef.terminate) map { _ =>
                sys.error("Do something useful with StoreFailure")
                val errors = if (overLarge.nonEmpty) List((state.ingested + toIngest.size, overLargeErrorBase))
                             else parsed.errors.map(pe => (pe.line, pe.msg))

                state.update(updatedParser, toIngest.size, errors)
              }
            } 
        }
      }

      errorHandling match {
        case StopOnFirstError => ingestUnbuffered(JSONParseState.empty(true), stream, storeMode.createStreamRef(false))
        case IngestAllPossible => ingestUnbuffered(JSONParseState.empty(false), stream, storeMode.createStreamRef(false))
        case AllOrNothing => ingestAllOrNothing(JSONParseState.empty(true), stream, storeMode.createStreamRef(false))
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
            case JSONParseState(_, ingested, errors) =>
              errorHandling match {
                case StopOnFirstError | AllOrNothing =>
                  StreamingResult(ingested, errors.headOption.map(_._2))

                case IngestAllPossible =>
                  BatchResult(ingested + errors.size, ingested, Vector(errors: _*))
              }
          }

        case GlobalDurability(jobId) =>
          ingestJSONChunk(errorHandling, storeMode, Some(jobId), dataStream) map {
            case JSONParseState(_, ingested, errors) =>
              BatchResult(ingested + errors.size, ingested, Vector(errors: _*))
          }
      } 
    }
  }
}
