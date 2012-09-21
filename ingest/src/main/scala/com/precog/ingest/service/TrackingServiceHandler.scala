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

import blueeyes.core.ByteChunk
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._

import blueeyes.json.JsonAST._

import java.nio.channels._
import java.nio.ByteBuffer

import com.weiglewilczek.slf4s.Logging

import scalaz.{Validation, Success}

class TrackingServiceHandler(accessControl: AccessControl[Future], eventStore: EventStore, usageLogging: UsageLogging, insertTimeout: Timeout)(implicit dispatcher: MessageDispatcher)
extends CustomHttpService[Future[ByteChunk], (Token, Path) => Future[HttpResponse[JValue]]] with Logging {

  def threadPool: ThreadPoolExecutor

  def writeChunkStream(chan: WriteableByteChannel, chunk: ByteChunk): Future[Unit] = {
    chan.write(ByteBuffer.wrap(chunk.data))
    chunk.next match {
      case Some(future) => future flatMap (writeChunk(chan, _))
      case None => Future {
        chan.close()
      }
    }
  }

  class EventQueueInserter(p: Path, t: Token, chan: ReadableByteChannel, batchComplete: List[Int] => Unit) extends Runnable {
    def run() {
      val reader = new BufferedReader(Channels.newReader(chan, "UTF-8"))
      val lines = Iterator.continually(reader.readLine()).takeWhile(_ != null)

      val futures: List[Future[Option[Int]]] = lines.map(JsonParser.parseOpt(_)).zipWithIndex.map {
        case (Some(event), _) =>
          val eventInstance = Event.fromJValue(p, event, t.tid)
          logger.trace("Saving event: " + eventInstance)
          eventStore.save(eventInstance, insertTimeout)
        case (_, line) =>
          Future { Some(line) }
      }.toList

      Future.sequence(futures) foreach { reuslts =>
        chan.close()
        val errors = results collect { case Some(line) => line }
        batchComplete(errors)
      }
    }
  }

  val service = (request: HttpRequest[Future[ByteChunk]]) => {
    Success { (t: Token, p: Path) =>
      accessControl.mayAccess(t.tid, p, Set(), WritePermission) flatMap { mayAccess =>
        if(mayAccess) {
          request.content map { futureContent =>
            try {
              if (sync) {
                val pipe = Pipe.open()
                val promise = Promise[List[Int]]()
                val inserter = new EventQueueInserter(p, t, pipe.source(), {
                  x => promise.complete(Right(x))
                })
                threadPool.execute(inserter)

                for {
                  byteStream <- futureContent
                  _ <- writeChunkStream(pipe.sink(), byteStream)
                  errors <- promise
                } yield {
                  if (errors.isEmpty) {
                    HttpResponse[JValue](OK)
                  } else {
                    HttpResponse[JValue](BadRequest, JObject(JField("errors", JArray(errors map (JNum(_))))))
                  }
                }

              } else {
                val file = File.createTempFile("async-ingest-", null)
                val outChannel = new FileOutputStream(file).getChannel()
                for {
                  bytesStream <- futureContent
                  _ <- writeChunkStream(outChannel, byteStream)
                } yield {
                  val inChannel = new FileInputStream(file).getChannel()
                  threadPool.execute(new EventQueueInserter(p, t, inChannel, _ => ()))
                  HttpResponse[JValue](Accepted)
                }
              }
            } catch {
              case ex => Future(HttpResponse[JValue](ServiceUnavailable))
            }
          } getOrElse {
            Future(HttpResponse[JValue](BadRequest, content=Some(JString("Missing event data."))))
          }
        } else {
          Future(HttpResponse[JValue](Unauthorized, content=Some(JString("Your token does not have permissions to write at this location."))))
        }
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
