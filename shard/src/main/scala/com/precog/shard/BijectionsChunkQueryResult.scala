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

import java.nio._
import java.nio.charset._

import blueeyes.core.http.{ HttpResponse, HttpFailure }
import blueeyes.core.data.{ Chunk, ByteChunk, Bijection, BijectionsChunkJson }
import blueeyes.json.{ JsonAST, JsonParser, Printer }
import blueeyes.json.JsonAST.{ JValue,  JArray  }
import blueeyes.json.Printer.{ compact, render }
import blueeyes.json.serialization.DefaultSerialization._

import akka.dispatch.Future
import scalaz._


object BijectionsChunkQueryResult {

  // Required by TokenServiceCombinator's `token` combinator.
  implicit val queryResultErrorTransform = (failure: HttpFailure, s: String) =>
    HttpResponse[QueryResult](failure, content = Some(Left(s.serialize)))

  // Required to map HttpServices resulting in JValue to ones that results in QueryResult.
  implicit def JValueResponseToQueryResultResponse(r1: Future[HttpResponse[JValue]]): Future[HttpResponse[QueryResult]] =
    r1 map { response => response.copy(content = response.content map (Left(_))) }

  implicit def QueryResultToChunk(implicit M: Monad[Future]) = new Bijection[Either[JValue, StreamT[Future, CharBuffer]], ByteChunk] {
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

    def unapply(chunk: ByteChunk): Either[JValue, StreamT[Future, CharBuffer]] = {
      def next(chunk: ByteChunk): StreamT.Step[CharBuffer, StreamT[Future, CharBuffer]] = {
        val json = new String(chunk.data)
        val buffer = CharBuffer.allocate(json.length)
        buffer.put(json)
        buffer.flip()

        val tail = StreamT(chunk.next match {
          case None => StreamT.Done.point[Future]
          case Some(futureChunk) => futureChunk map (next(_))
        })

        StreamT.Yield(buffer, tail)
      }

      // All chunks generated from Streams have at least 2 parts, otherwise, if
      // we have one, we can just slurp the entire chunk up as JSON.

      chunk.next map { _ =>
        Right(StreamT(next(chunk).point[Future]))
      } getOrElse {
        Left(JValueToChunk.unapply(chunk))
      }
    }

    def apply(queryResult: Either[JValue, StreamT[Future, CharBuffer]]): ByteChunk = {
      val encoder = Charset.forName("UTF-8").newEncoder
      
      def next(chunked: StreamT[Future, CharBuffer]): Future[ByteChunk] = {
        chunked.uncons flatMap {
          case Some((chunk, chunkStream)) =>
            val buffer = encoder.encode(chunk)
            
            val array = new Array[Byte](buffer.remaining())
            buffer.get(array)
            
            if (array.length > 0)
              M.point(Chunk(array, Some(next(chunkStream))))
            else
              next(chunkStream)

          case None => M.point(Chunk("]".getBytes(DefaultCharset), None))
        }
      }

      queryResult match {
        case Left(jval) =>
          JValueToChunk(jval)
        case Right(chunkedQueryResult) =>
          Chunk("[".getBytes(DefaultCharset), Some(next(chunkedQueryResult)))
      }
    }
  }
}
