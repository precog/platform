package com.precog.util

import scala.annotation.tailrec
import scala.{ specialized => spec }

import scalaz._
import scalaz.syntax.monad._

import blueeyes.json._
import blueeyes.json.AsyncParser._
import blueeyes.core.data._

import java.nio.ByteBuffer
import akka.dispatch.{Future, Await, ExecutionContext}

/**
 * This object contains some methods to do faster iteration over primitives.
 *
 * In particular it doesn't box, allocate intermediate objects, or use a (slow)
 * shared interface with scala collections.
 */
object JsonUtil {
  def parseSingleFromByteChunk(bc: ByteChunk)(implicit M: Monad[Future]): Future[Validation[Seq[Throwable], JValue]] =
    parseSingleFromStream[Future](bc.fold(_ :: StreamT.empty, identity))

  def parseSingleFromStream[M[+_]: Monad](stream: StreamT[M, Array[Byte]]): M[Validation[Seq[Throwable], JValue]] = {
    def rec(stream: StreamT[M, Array[Byte]], parser: AsyncParser): M[Validation[Seq[Throwable], JValue]] = {
      def handle(ap: AsyncParse, next: => M[Validation[Seq[Throwable], JValue]]): M[Validation[Seq[Throwable], JValue]] = ap match {
        case AsyncParse(errors, _) if errors.nonEmpty =>
          Failure(errors).point[M]
        case AsyncParse(_, values) if values.nonEmpty =>
          Success(values.head).point[M]
        case _ =>
          next
      }

      stream.uncons flatMap {
        case Some((bytes, tail)) =>
          val (ap, p2) = parser(More(ByteBuffer.wrap(bytes)))
          handle(ap, rec(tail, p2))
        case None =>
          val (ap, p2) = parser(Done)
          handle(ap, Failure(Seq(new Exception("parse failure"))).point[M])
      }
    }

    rec(stream, AsyncParser.json())
  }


  def parseManyFromByteChunk(bc: ByteChunk)(implicit M: Monad[Future]): StreamT[Future, AsyncParse] =
    parseManyFromStream[Future](bc.fold(_ :: StreamT.empty, identity))

  def parseManyFromStream[M[+_]: Monad](stream: StreamT[M, Array[Byte]]): StreamT[M, AsyncParse] = {
    // create a new stream, using the current stream and parser
    StreamT.unfoldM((stream, AsyncParser.stream())) {
      case (stream, parser) => stream.uncons map {
        case Some((bytes, tail)) =>
          // parse the current byte buffer, keeping track of the
          // new parser instance we were given back
          val (r, parser2) = parser(More(ByteBuffer.wrap(bytes)))
          Some((r, (tail, parser2)))
        case None =>
          // once we're out of byte buffers, send None to signal EOF
          val (r, parser2) = parser(Done)
          Some((r, (StreamT.empty, parser2)))
      }
    }
  }
}
