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
package com.reportgrid.storage
package kafka

import leveldb._
import Bijection._

import akka.actor.Actor
import akka.actor.ActorRef

import blueeyes.json.JsonAST._
import blueeyes.persistence.cache.Cache
import blueeyes.persistence.cache.CacheSettings
import blueeyes.persistence.cache.ExpirationPolicy

import com.querio.ingest.api._
import com.reportgrid.analytics.Path
import com.weiglewilczek.slf4s._

import _root_.kafka.consumer._

import java.io.File
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.TimeUnit

import scalaz._
import scalaz.syntax.std.optionV._
import scalaz.effect._



case object Stop

class RoutingActor(baseDir: File) extends Actor with Logging {
  implicit val lengthEncoder: LengthEncoder = null

  val routing = Cache.concurrent[ProjectionDescriptor, ActorRef](
    CacheSettings(
      expirationPolicy = ExpirationPolicy(None, None, TimeUnit.SECONDS), 
      evict = { 
        (descriptor, actor) => descriptor.sync.map(_ => actor ! Stop).unsafePerformIO
      }
    )
  )

  def receive = {
    case SyncMessage(producerId, syncId, eventIds) => //TODO

    case ev @ EventMessage(_, _, Event(pathName, _, data)) =>
      val path = Path(pathName)
      for {
        (selector, jvalue) <- data.flattenWithPath
        columnType <- ColumnType.forValue(jvalue)
      } {
        val dataShape = FileProjectionDescriptor(baseDir, path, List(ColumnDescriptor(selector, columnType)), 1)
        val comparator = ProjectionComparator.forProjection(dataShape)
        val actor = routing.get(dataShape).toSuccess(new RuntimeException("No cached actor available."): Throwable).toValidationNel.orElse(
          //todo: add selector metadata to column
          LevelDBProjection(new File(baseDir, path.path), Some(comparator)).map(p => Actor.actorOf(new ProjectionActor(p, dataShape)))
        )

        actor match {
          case Success(actor) =>
            routing.putIfAbsent(dataShape, actor)
            actor ! ProjectionInsert(ev.uid, jvalue)

          case Failure(errors) => 
            for (t <- errors.list) logger.error("Could not obtain actor for projection: " , t)
        }
      }
  }
}

case class ProjectionInsert(id: Long, jvalue: JValue)

class ProjectionActor(projection: LevelDBProjection, descriptor: ProjectionDescriptor) extends Actor {
  implicit val bijection: Bijection[JValue, ByteBuffer] = projectionBijection(descriptor)

  def receive = {
    case Stop => //close the db
      projection.close.unsafePerformIO

    case ProjectionInsert(id, jvalue) => 
      projection.insert(id, jvalue.as[ByteBuffer]).unsafePerformIO
  }
}

object ColumnConsumer {
  def main(argv: Array[String]) = {

    val props = new Properties()
    props.put("zk.connect", "localhost:2181")
    props.put("zk.connectiontimeout.ms", "1000000")
    props.put("groupid", "projections")

    val conf = new ConsumerConfig(props)
    val consumer = Consumer.create(conf)

    val streams = consumer.createMessageStreams(Map("raw" -> 1))
    val router = Actor.actorOf(new RoutingActor(new File(".")))

    // accumulate state, updating the metadata every time you get a sync
    for (rawStreams <- streams.get("raw"); stream <- rawStreams; message <- stream) {
      router ! IngestMessageSerialization.readMessage(message.buffer) 
    }
  }
}



// vim: set ts=4 sw=4 et:
