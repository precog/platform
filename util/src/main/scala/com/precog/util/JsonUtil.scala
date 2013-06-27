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
package com.precog.util

import scala.annotation.tailrec
import scala.{ specialized => spec }

import scalaz._

import blueeyes.json._
import blueeyes.core.data._
import AsyncParser._

import java.nio.ByteBuffer
import akka.dispatch.{Future, Await, ExecutionContext}

/**
 * This object contains some methods to do faster iteration over primitives.
 *
 * In particular it doesn't box, allocate intermediate objects, or use a (slow)
 * shared interface with scala collections.
 */
object JsonUtil {

  def parseSingleFromByteChunk(bc: ByteChunk)
    (implicit M: Monad[Future]): Future[Validation[Seq[Throwable], JValue]] =
    parseSingleFromStream[Future](bc.fold(_ :: StreamT.empty, identity))

  def parseSingleFromStream[M[+_]](stream: StreamT[M, ByteBuffer])
    (implicit M: Monad[M]): M[Validation[Seq[Throwable], JValue]] = {

    // use an empty byte buffer to "prime" the async parser
    val p = AsyncParser.stream()

    def xyz(stream: StreamT[M, ByteBuffer], p: AsyncParser):
        M[Validation[Seq[Throwable], JValue]] = {
      def handle(ap: AsyncParse, next: => M[Validation[Seq[Throwable], JValue]]):
          M[Validation[Seq[Throwable], JValue]] =
        ap match {
          case AsyncParse(errors, _) if !errors.isEmpty =>
            M.point(Failure(errors))
          case AsyncParse(_, values) if !values.isEmpty =>
            M.point(Success(values.head))
          case _ =>
            next
        }

      M.bind(stream.uncons) {
        case Some((bb, tail)) =>
          val (ap, p2) = p(Some(bb))
          handle(ap, xyz(tail, p2))
        case None =>
          val (ap, p2) = p(None)
          handle(ap, M.point(Failure(Seq(new Exception("parse failure")))))
      }
    }
    xyz(stream, p)
  }


  def parseManyFromByteChunk(bc: ByteChunk)
    (implicit M: Monad[Future]): StreamT[Future, AsyncParse] =
    parseManyFromStream[Future](bc.fold(_ :: StreamT.empty, identity))

  def parseManyFromStream[M[+_]]
    (stream: StreamT[M, ByteBuffer])
    (implicit M: Monad[M]): StreamT[M, AsyncParse] = {

    val p = AsyncParser.stream()

    // create a new stream, using the current stream and parser
    StreamT.unfoldM((stream, p)) {
      case (stream, parser) => M.map(stream.uncons) {
        case Some((bb, tail)) =>
          // parse the current byte buffer, keeping track of the
          // new parser instance we were given back
          val (r, parser2) = parser(Some(bb))
          Some((r, (tail, parser2)))
        case None =>
          // once we're out of byte buffers, send None to signal EOF
          val (r, parser2) = parser(None)
          Some((r, (StreamT.empty, parser2)))
      }
    }
  }
}
