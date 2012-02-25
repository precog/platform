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
package daze

import yggdrasil._

import akka.actor.ActorSystem
import akka.dispatch.Await
import akka.dispatch.Future
import akka.dispatch.ExecutionContext
import akka.util.duration._

import org.specs2.ScalaCheck
import org.specs2.matcher.ThrownMessages
import org.specs2.mutable.{BeforeAfter,Specification}
import org.specs2.specification.Scope
import org.scalacheck.{Arbitrary,Gen}
import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.list._
import scalaz.std.string._
import scalaz.std.anyVal._
import scalaz.syntax.order._
import scalaz.syntax.semigroup._
import Ordering._
import Iteratee._
import MonadPartialOrder._

class YggdrasilEnumOpsComponentSpec extends Specification with YggdrasilEnumOpsComponent with Logging {
  type MemoContext = MemoizationContext
  type YggConfig = YggEnumOpsConfig

  implicit val actorSystem: ActorSystem = ActorSystem("yggdrasil_ops_spec")
  implicit def asyncContext = ExecutionContext.defaultExecutionContext

  object yggConfig extends YggConfig {
    def sortBufferSize = 10
    def sortWorkDir = sys.error("not used")
    def flatMapTimeout = intToDurationInt(30).seconds
  }

  implicit val chunkSerialization = SimpleProjectionSerialization
  val memoizationContext = MemoizationContext.Noop
  object ops extends Ops

  implicit val keyOrder: Order[ops.Key] = new Order[ops.Key] {
    def order(k1: ops.Key, k2: ops.Key) = (k1.size ?|? k2.size) |+| (k1 zip k2).foldLeft[Ordering](EQ) {
      case (ord, (v1, v2)) => ord |+| (v1 ?|? v2)
    }
  }

  "sort" should {
    "sort values" in {
      implicit val SEventOrder: Order[SEvent] = Order[String].contramap((_: SEvent)._2.mapStringOr("")(a => a))
      val enumP = enumPStream[Unit, Vector[SEvent], IO](Stream(Vector(SEvent(Vector(), SString("2")), SEvent(Vector(), SString("3"))), Vector(SEvent(Vector(), SString("1")))))
      val sorted = Await.result(ops.sort(DatasetEnum(Future(enumP)), None).fenum, intToDurationInt(30).seconds)

      (consume[Unit, Vector[SEvent], IO, List] &= sorted[IO])
      .run(_ => sys.error("...")).unsafePerformIO.flatten.map(_._2.mapStringOr("wrong")(a => a)) must_== List("1", "2", "3")
    }
  }

  "group" should {
    "group values" in {

      val enumP = enumPStream[Unit, Vector[SEvent], IO](Stream(Vector(SEvent(Vector(), SString("2")), SEvent(Vector(), SString("3"))), Vector(SEvent(Vector(), SString("1")))))
      val keyf: SEvent => List[SValue] = { 
        ev => ev._2.mapStringOr(List(SInt(0)))(s => List(SInt(s.toInt % 2)))
      }

      val grouped = Await.result((ops.group(DatasetEnum(Future(enumP), None))(keyf)), intToDurationInt(30).seconds)

      val groups = (consume[Unit, (List[SInt], DatasetEnum[Unit, Vector[SEvent], IO]), IO, List] &= grouped[IO]).runOrZero.unsafePerformIO
      groups must haveSize(2)
    }
  }
}

// vim: set ts=4 sw=4 et:
