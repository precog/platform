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
package com.precog
package ingest
package service

import common._
import common.security._

import akka.dispatch.{ Future, Promise }
import akka.dispatch.MessageDispatcher
import akka.util.Timeout

import blueeyes.core.data.ByteChunk
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._

import blueeyes.json.JsonParser
import blueeyes.json.JsonAST._

import java.util.concurrent.{ ThreadPoolExecutor, TimeUnit, ArrayBlockingQueue, RejectedExecutionException }

import java.io.{ File, BufferedReader, FileInputStream, FileOutputStream }
import java.nio.channels._
import java.nio.ByteBuffer

import com.weiglewilczek.slf4s.Logging

import scalaz.{Validation, Success}

class TrackingServiceHandler(accessControl: AccessControl[Future], eventStore: EventStore, usageLogging: UsageLogging, insertTimeout: Timeout, maxReadThreads: Int)(implicit dispatcher: MessageDispatcher)
extends CustomHttpService[Either[Future[JValue], ByteChunk], (Token, Path) => Future[HttpResponse[JValue]]] with Logging {

  // TODO Make this more configurable?
  def threadPool: ThreadPoolExecutor = new ThreadPoolExecutor(2, maxReadThreads, 5, TimeUnit.SECONDS, new ArrayBlockingQueue(50))

  def writeChunkStream(chan: WritableByteChannel, chunk: ByteChunk): Future[Unit] = {
    chan.write(ByteBuffer.wrap(chunk.data))
    chunk.next match {
      case Some(future) => future flatMap (writeChunkStream(chan, _))
      case None => Future {
        chan.close()
      }
    }
  }

  def ingest(p: Path, t: Token, event: JValue): Future[Unit] = {
    val eventInstance = Event.fromJValue(p, event, t.tid)
    logger.trace("Saving event: " + eventInstance)
    eventStore.save(eventInstance, insertTimeout)
  }

  class EventQueueInserter(p: Path, t: Token, chan: ReadableByteChannel, batchComplete: List[Int] => Unit) extends Runnable {
    def run() {
      val reader = new BufferedReader(Channels.newReader(chan, "UTF-8"))
      val lines = Iterator.continually(reader.readLine()).takeWhile(_ != null)

      val futures: List[Future[Option[Int]]] = lines.map(JsonParser.parseOpt(_)).zipWithIndex.map {
        case (Some(event), _) => ingest(p, t, event) map (_ => None)
        case (_, line) => Future { Some(line) }
      }.toList

      Future.sequence(futures) foreach { results =>
        chan.close()
        val errors = results collect { case Some(line) => line }
        batchComplete(errors)
      }
    }
  }

  def asyncIngest(byteStream: ByteChunk, t: Token, p: Path): Future[HttpResponse[JValue]] = {
    val file = File.createTempFile("async-ingest-", null)
    val outChannel = new FileOutputStream(file).getChannel()
    for {
      _ <- writeChunkStream(outChannel, byteStream)
    } yield {
      val inChannel = new FileInputStream(file).getChannel()
      try {
        threadPool.execute(new EventQueueInserter(p, t, inChannel, _ => ()))
        HttpResponse[JValue](Accepted)
      } catch {
        case _: RejectedExecutionException => HttpResponse[JValue](ServiceUnavailable)
      }
    }
  }

  def syncIngest(byteStream: ByteChunk, t: Token, p: Path): Future[HttpResponse[JValue]] = {
    try {
      val pipe = Pipe.open()
      val promise = Promise[List[Int]]()
      val inserter = new EventQueueInserter(p, t, pipe.source(), { errors =>
        promise.complete(Right(errors))
      })
      threadPool.execute(inserter)

      for {
        _ <- writeChunkStream(pipe.sink(), byteStream)
        errors <- promise
      } yield {
        if (errors.isEmpty) {
          HttpResponse[JValue](OK)
        } else {
          HttpResponse[JValue](BadRequest, content = Some(JObject(JField("errors", JArray(errors map (JNum(_)))) :: Nil)))
        }
      }
    } catch {
      case _: RejectedExecutionException => Future(HttpResponse[JValue](ServiceUnavailable))
    }
  }

  val service = (request: HttpRequest[Either[Future[JValue], ByteChunk]]) => {
    Success { (t: Token, p: Path) =>
      accessControl.mayAccess(t.tid, p, Set(), WritePermission) flatMap {
        case true => try {
          request.content map {
            case Left(futureEvent) =>
              for {
                event <- futureEvent
                _ <- ingest(p, t, event)
              } yield HttpResponse[JValue](OK)

            case Right(byteStream) =>
              val async = request.parameters.get('sync) map (_ == "async") getOrElse false
              if (async) {
                asyncIngest(byteStream, t, p)
              } else  {
                syncIngest(byteStream, t, p)
              }

          } getOrElse {
            Future(HttpResponse[JValue](BadRequest, content=Some(JString("Missing event data."))))
          }
        } catch {
          case _ => Future(HttpResponse[JValue](ServiceUnavailable))
        }

        case false =>
          Future(HttpResponse[JValue](Unauthorized, content=Some(JString("Your token does not have permissions to write at this location."))))
      }
    }
  }

  val metadata = Some(DescriptionMetadata(
    """
      This service can be used to store an data point with or without an associated timestamp. 
      Timestamps are not added by default.
    """
  ))
}
