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
