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
package com.precog.shard

import java.io.{ ByteArrayOutputStream, OutputStreamWriter }

import blueeyes.core.http.{ HttpResponse, HttpFailure }
import blueeyes.core.data.{ Chunk, ByteChunk, Bijection, BijectionsChunkJson }
import blueeyes.json.{ JsonAST, JsonParser, Printer }
import blueeyes.json.JsonAST.{ JValue,  JArray  }
import blueeyes.json.Printer.{ compact, render }
import blueeyes.json.xschema.DefaultSerialization._

import akka.dispatch.Future
import scalaz._


object BijectionsChunkQueryResult {

  // Required by TokenServiceCombinator's `token` combinator.
  implicit val queryResultErrorTransform = (failure: HttpFailure, s: String) =>
    HttpResponse[QueryResult](failure, content = Some(Left(s.serialize)))

  // Required to map HttpServices resulting in JValue to ones that results in QueryResult.
  implicit def JValueResponseToQueryResultResponse(r1: Future[HttpResponse[JValue]]): Future[HttpResponse[QueryResult]] =
    r1 map { response => response.copy(content = response.content map (Left(_))) }

  implicit def QueryResultToChunk(implicit M: Monad[Future]) = new Bijection[Either[JValue, StreamT[Future, List[JValue]]], ByteChunk] {
    import scalaz.syntax.monad._
    import BijectionsChunkJson.JValueToChunk

    val DefaultCharset = "UTF-8"
    private val Comma = ','.toInt

    private object PartialJArray {
      def unapply(s: String): Option[String] = if (s.startsWith(",")) {
        Some("[%s]" format s.substring(1))
      } else {
        Some("[%s]" format s)
      }
    }

    def unapply(chunk: ByteChunk): Either[JValue, StreamT[Future, List[JValue]]] = {
      def next(chunk: ByteChunk): StreamT.Step[List[JValue], StreamT[Future, List[JValue]]] = {
        val json = new String(chunk.data)

        val jvals = json match {
          case "[" | "]" => None
          case PartialJArray(jarray) =>
            JsonParser.parse(jarray) match {
              case JArray(jvals) => Some(jvals)
              case jval => sys.error("Expected a JArray, but found %s" format jval)
            }
        }

        val tail = StreamT(chunk.next match {
          case None => StreamT.Done.point[Future]
          case Some(futureChunk) => futureChunk map (next(_))
        })

        jvals map (StreamT.Yield(_, tail)) getOrElse StreamT.Skip(tail)
      }

      // All chunks generated from Streams have at least 2 parts, otherwise, if
      // we have one, we can just slurp the entire chunk up as JSON.

      chunk.next map { _ =>
        Right(StreamT(next(chunk).point[Future]))
      } getOrElse {
        Left(JValueToChunk.unapply(chunk))
      }
    }

    def apply(queryResult: Either[JValue, StreamT[Future, List[JValue]]]): ByteChunk = {
      def next(chunked: StreamT[Future, List[JValue]], first: Boolean = false): Future[ByteChunk] = {
        chunked.uncons map {
          case Some((chunk, chunkStream)) =>
            val stream = new ByteArrayOutputStream()
            val writer = new OutputStreamWriter(stream, DefaultCharset)
            chunk match {
              case jval :: jvals =>
                if (!first) writer.write(Comma)

                compact(render(jval), writer)
                jvals foreach { jval =>
                  writer.write(Comma)
                  compact(render(jval), writer)
                }

                Chunk(stream.toByteArray, Some(next(chunkStream, first = false)))

              case Nil =>
                Chunk(new Array[Byte](0), Some(next(chunkStream, first)))
            }

          case None => Chunk("]".getBytes(DefaultCharset), None)
        }
      }

      queryResult match {
        case Left(jval) =>
          JValueToChunk(jval)
        case Right(chunkedQueryResult) =>
          Chunk("[".getBytes(DefaultCharset), Some(next(chunkedQueryResult, first = true)))
      }
    }
  }
}
