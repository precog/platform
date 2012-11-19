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
import blueeyes.core.data.{ Chunk, ByteChunk, Bijection, DefaultBijections }
import blueeyes.json.{ JArray, JParser, JValue }
import blueeyes.json.serialization.DefaultSerialization._

import akka.dispatch.{ Await, Future }
import akka.util.Duration

import scalaz._

object BijectionsChunkQueryResult {

  // Required by APIKeyServiceCombinator's `apiKey` combinator.
  implicit val queryResultErrorTransform = (failure: HttpFailure, s: String) =>
    HttpResponse[QueryResult](failure, content = Some(Left(s.serialize)))

  // Required to map HttpServices resulting in JValue to ones that results in QueryResult.
  implicit def JValueResponseToQueryResultResponse(r1: Future[HttpResponse[JValue]]): Future[HttpResponse[QueryResult]] =
    r1 map { response => response.copy(content = response.content map (Left(_))) }

  implicit def QueryResultToChunk(implicit M: Monad[Future]) = new Bijection[Either[JValue, StreamT[Future, CharBuffer]], Future[ByteChunk]] {
    import scalaz.syntax.monad._
    import DefaultBijections._

    private object PartialJArray {
      def unapply(s: String): Option[String] = if (s.startsWith(",")) {
        Some("[%s]" format s.substring(1))
      } else {
        Some("[%s]" format s)
      }
    }

    def unapply(chunkM: Future[ByteChunk]): Either[JValue, StreamT[Future, CharBuffer]] = {
      import com.precog.util.JsonUtil
      val utf8 = java.nio.charset.Charset.forName("UTF-8")

      val backM = chunkM.map {
        case Left(bb) =>
          val v = JParser.parseFromByteBuffer(bb)
          Left(v.valueOr(throw _))
        case Right(stream) =>
          Right(stream.map(utf8.decode))
      }

      Await.result(backM, Duration(1, "seconds"))
    }

    def apply(queryResult: Either[JValue, StreamT[Future, CharBuffer]]): Future[ByteChunk] = {
      val encoder = Charset.forName("UTF-8").newEncoder
      
      val Threshold = 10000000     // 10 million characters should be enough for anyone...
      
      def page(chunked: StreamT[Future, CharBuffer], acc: Vector[CharBuffer], size: Int): Future[(Vector[CharBuffer], Int)] = {
        chunked.uncons flatMap {
          case Some((chunk, tail)) => {

            if (size > Threshold) {
              val buffer = RecoverJson.getJsonCloserBuffer(acc)
              M.point((acc :+ buffer, size + buffer.remaining()))
            } else {
              val acc2 = acc :+ chunk
              val size2 = size + chunk.remaining()
              page(tail, acc2, size2)
            }
          }
          
          case None => {
            val buffer = CharBuffer.allocate(1)
            buffer.put(']')
            buffer.flip()
            M.point((acc :+ buffer, size + 1))
          }
        }
      }
      
      def collapse(acc: Vector[CharBuffer], size: Int): CharBuffer = {
        val back = CharBuffer.allocate(size)
        
        acc foreach { buffer =>
          back.put(buffer)
          buffer.flip()
        }
        
        back.flip()
        back
      }

      queryResult match {
        case Left(jval) =>
          M.point(Left(ByteBuffer.wrap(jval.renderCompact.getBytes)))
        
        case Right(chunkedQueryResult) => {
          val buffer = CharBuffer.allocate(1)
          buffer.put('[')
          buffer.flip()
          
          val resultF = page(chunkedQueryResult, Vector(buffer), 1) map Function.tupled(collapse)
          
          resultF.map(buffer => Left(encoder.encode(buffer)))
        }
      }
    }
  }
}
