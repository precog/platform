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
package com.precog.yggdrasil
package kafka

import com.precog.util._
import com.precog.common.util.FixMe._
import leveldb._
import shard._
import Bijection._

import com.precog.common._

import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorRef
import akka.util.Timeout
import akka.util.duration._

import blueeyes.json.JsonAST._
import blueeyes.persistence.cache.Cache
import blueeyes.persistence.cache.CacheSettings
import blueeyes.persistence.cache.ExpirationPolicy

import com.weiglewilczek.slf4s._

import _root_.kafka.consumer._

import java.io.File
import java.io.FileInputStream
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection._

import scalaz._
import scalaz.syntax.std.booleanV._
import scalaz.syntax.std.optionV._
import scalaz.syntax.validation._
import scalaz.effect._
import scalaz.iteratee.EnumeratorT
import scalaz.MonadPartialOrder._


case class ProjectionInsert(identities: Identities, values: Seq[CValue])

case object ProjectionGet

trait ProjectionResults {
  val desc : ProjectionDescriptor
  def enumerator : EnumeratorT[Unit, Seq[CValue], IO]
}

class ProjectionActor(val projection: LevelDBProjection, descriptor: ProjectionDescriptor) extends Actor {
  def asCValue(jval: JValue): CValue = jval match { 
    case JString(s) => CString(s)
    case JInt(i)    => CNum(BigDecimal(i))
    case JDouble(d) => CDouble(d)
    case JBool(b)   => CBoolean(b)
    case JNull      => CNull
    case x          => sys.error("JValue type not yet supported: " + x.getClass.getName )
  }

  def receive = {
    case Stop => //close the db
      projection.close.unsafePerformIO

    case ProjectionInsert(identities, values) => 
      projection.insert(identities, values).unsafePerformIO

    case ProjectionGet => 
      sender ! projection
  }
}

// vim: set ts=4 sw=4 et:
