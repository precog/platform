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
package yggdrasil
package actor

import leveldb._

import akka.actor.Actor
import akka.actor.Scheduler
import akka.util.duration._

import blueeyes.json.JsonAST._

import com.weiglewilczek.slf4s._

import scala.annotation.tailrec
import scalaz.effect._
import scalaz.iteratee.EnumeratorT

case object Stop

case object IncrementRefCount
case object DecrementRefCount

case class ProjectionInsert(identities: Identities, values: Seq[CValue])
case class ProjectionBatchInsert(insert: Seq[ProjectionInsert])

case object ProjectionGet

trait ProjectionResults {
  val desc : ProjectionDescriptor
  def enumerator : EnumeratorT[Unit, Seq[CValue], IO]
}

class ProjectionActor(val projection: LevelDBProjection, descriptor: ProjectionDescriptor, scheduler: Scheduler) extends Actor with Logging {
  def asCValue(jval: JValue): CValue = jval match { 
    case JString(s) => CString(s)
    case JInt(i)    => CNum(BigDecimal(i))
    case JDouble(d) => CDouble(d)
    case JBool(b)   => CBoolean(b)
    case JNull      => CNull
    case x          => sys.error("JValue type not yet supported: " + x.getClass.getName )
  }

  var refCount = 0

  def insertAll(batch: Seq[ProjectionInsert]) = {
    @tailrec def step(iter: Iterator[ProjectionInsert]) {
      if (iter.hasNext) {
        val insert = iter.next
        try {
          projection.insert(insert.identities, insert.values).unsafePerformIO
        } catch {
          case ex => println("blah")
        }
        step(iter)
      }
    }
    step(batch.iterator)
  }

  def receive = {
    case Stop => //close the db
      if(refCount == 0) {
        logger.debug("Closing projection.")
        projection.close.unsafePerformIO
      } else {
        logger.debug("Deferring close ref count [%d]".format(refCount))
        scheduler.scheduleOnce(1 second, self, Stop) 
      }

    case IncrementRefCount => refCount += 1

    case DecrementRefCount => refCount -= 1

    case ProjectionInsert(identities, values) => 
      //logger.debug("Projection insert")
      projection.insert(identities, values).unsafePerformIO
      sender ! ()
    
    case ProjectionBatchInsert(inserts) =>
      insertAll(inserts)
      sender ! ()

    case ProjectionGet => 
      sender ! projection
  }
}

// vim: set ts=4 sw=4 et:
