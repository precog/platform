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

import annotation.tailrec
import collection.immutable.Stack

import java.nio._
import java.nio.charset._

import blueeyes.core.http.{ HttpResponse, HttpFailure }
import blueeyes.core.data.{ Chunk, ByteChunk, Bijection, BijectionsChunkJson }
import blueeyes.json.{ JsonAST, JsonParser, Printer }
import blueeyes.json.JsonAST.{ JValue,  JArray  }
import blueeyes.json.Printer.{ compact, render }
import blueeyes.json.serialization.DefaultSerialization._

import akka.dispatch.{Await, Future}
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
    import BijectionsChunkJson.JValueToChunk

    private object PartialJArray {
      def unapply(s: String): Option[String] = if (s.startsWith(",")) {
        Some("[%s]" format s.substring(1))
      } else {
        Some("[%s]" format s)
      }
    }

    def unapply(chunkM: Future[ByteChunk]): Either[JValue, StreamT[Future, CharBuffer]] = {
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
      
      val backM = chunkM map { chunk =>
        chunk.next map { _ =>
          Right(StreamT(next(chunk).point[Future]))
        } getOrElse {
          Left(JValueToChunk.unapply(chunk))
        }
      }
      
      Await.result(backM, Duration(1, "seconds"))
    }

    sealed trait Balanced
    case object Brace extends Balanced
    case object Bracket extends Balanced
    case class Colon(v: Balanced) extends Balanced
    case object Quote extends Balanced
    case object NullValue extends Balanced

    @tailrec private def findEndString(buffers: Vector[CharBuffer], bufferIndex: Int, offset: Int): Option[(Int, Int)] = {
      if (bufferIndex >= buffers.length)
        None
      else if (offset >= buffers(bufferIndex).limit)
        findEndString(buffers, bufferIndex + 1, offset % buffers(bufferIndex).limit)
      else {
        val char = buffers(bufferIndex).get(offset)

        if (char == '"')
          Some((bufferIndex, offset))
        else if (char == '\\')
          findEndString(buffers, bufferIndex, offset + 2)
        else
          findEndString(buffers, bufferIndex, offset + 1)
      }
    }

    private case class BalancedStackState(bufferIndex: Int, offset: Int, stack: Stack[Balanced]) {
      def increment(balanced: Balanced) = BalancedStackState(
        bufferIndex,
        offset + 1,
        stack push balanced
      )
      def decrement = BalancedStackState(
        bufferIndex,
        offset + 1,
        stack pop
      )
      def skip = BalancedStackState(
        bufferIndex,
        offset + 1,
        stack
      )
    }

    // TODO: Really needs unit tests
    // TODO: Consider trailing commas
    private def balancedStack(buffers: Vector[CharBuffer]) = {
      @tailrec @inline def buildState(accum: BalancedStackState): BalancedStackState =
        if (accum.bufferIndex >= buffers.length)
          accum
        else if (accum.offset >= buffers(accum.bufferIndex).limit)
          buildState(BalancedStackState(
            accum.bufferIndex + 1,
            accum.offset % buffers(accum.bufferIndex).limit,
            accum.stack
          ))
        else {
          val buffer = buffers(accum.bufferIndex)
          var char = buffer.get(accum.offset)
          char match {
            case '{' =>
              val next = accum increment Brace
              buildState(next.copy(stack = next.stack push Colon(NullValue)))
            case '[' =>
              buildState(accum increment Bracket)

            case '}' =>
              // Assumption: will never output valid {}
              buildState(accum decrement)
            case ']' =>
              buildState(accum decrement)

            case ',' if accum.stack.nonEmpty && accum.stack.head == Brace =>
              buildState(accum increment Colon(NullValue))
            case ':' =>
              // TODO: Consider {"abc":
              buildState(accum decrement)

            case '"' =>
              buildState(findEndString(buffers, accum.bufferIndex, accum.offset + 1) map { case (bufferIndex, offset) =>
                // Jump over the string
                BalancedStackState(
                  bufferIndex,
                  offset + 1,
                  accum.stack
                )
              } getOrElse {
                // String didn't end
                // TODO: Consider if the last character the escape character (\)
                BalancedStackState(
                  buffers.length,
                  buffer.limit,
                  accum.stack push Quote
                )
              })

            case _ => buildState(accum.skip)
          }
        }

      buildState(BalancedStackState(0, 0, new Stack())).stack
    }

    // Count braces, quotes and parens in every chunk's CharBuffer.
    // Creates a new buffer which can correctly close the chunk.
    private def getJsonCloserBuffer(buffers: Vector[CharBuffer]) = {
      val BlankElement = "null"

      def balancedToString(b: Balanced): String = b match {
        case Brace => "}"
        case Bracket => "]"
        case Colon(v) => ":" + balancedToString(v)
        case Quote => "\""
        case NullValue => "null"
      }

      val stringStack = balancedStack(buffers).map(balancedToString)
      // "}] <- stringStack
      // ,null <- 5
      val closerBuffer = CharBuffer.allocate(stringStack.map(_.length).sum + 5)

      @tailrec def addToCloserBuffer(s: Stack[String]) {
        if (s.length == 1) {
          // Last element should always be a Bracket (']')
          // Put the blank element before end of array
          closerBuffer.put(',')
          closerBuffer.put(BlankElement)
        }
        if (!s.isEmpty) {
          closerBuffer.put(s.head)
          addToCloserBuffer(s.pop)
        }
      }

      addToCloserBuffer(stringStack)
      closerBuffer.flip()
      closerBuffer
    }

    def apply(queryResult: Either[JValue, StreamT[Future, CharBuffer]]): Future[ByteChunk] = {
      val encoder = Charset.forName("UTF-8").newEncoder
      
      val Threshold = 10000000     // 10 million characters should be enough for anyone...
      
      def page(chunked: StreamT[Future, CharBuffer], acc: Vector[CharBuffer], size: Int): Future[(Vector[CharBuffer], Int)] = {
        chunked.uncons flatMap {
          case Some((chunk, tail)) => {

            if (size > Threshold) {
              val buffer = getJsonCloserBuffer(acc)
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
          M.point(JValueToChunk(jval))
        
        case Right(chunkedQueryResult) => {
          val buffer = CharBuffer.allocate(1)
          buffer.put('[')
          buffer.flip()
          
          val resultF = page(chunkedQueryResult, Vector(buffer), 1) map Function.tupled(collapse)
          
          resultF map { buffer =>
            val byteBuffer = encoder.encode(buffer)
            
            val array = new Array[Byte](byteBuffer.remaining())
            byteBuffer.get(array)
            
            Chunk(array, None)
          }
        }
      }
    }
  }
}
