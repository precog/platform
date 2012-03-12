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

import org.specs2.execute.Pending
import org.specs2.ScalaCheck
import org.specs2.matcher.ThrownMessages
import org.specs2.mutable.{BeforeAfter,Specification}
import org.specs2.specification.Scope
import org.scalacheck.{Arbitrary,Gen}
import Gen._
import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.list._
import scalaz.std.string._
import scalaz.std.iterable._
import scalaz.std.anyVal._
import scalaz.syntax.order._
import scalaz.syntax.semigroup._
import Ordering._
import Iteratee._
import MonadPartialOrder._

import com.precog.common.VectorCase

class YggdrasilEnumOpsComponentSpec extends Specification with YggdrasilEnumOpsComponent with Logging with ScalaCheck with ArbitrarySValue{
  type MemoContext = MemoizationContext
  type YggConfig = YggEnumOpsConfig

  def genChunks(size: Int) = LimitList.genLimitList[Vector[SEvent]](size) map { ll =>
    val (_, acc) = ll.values.foldLeft((Set.empty[Identities], List.empty[Vector[SEvent]])) {
      case ((allIds, acc), v) => 
        val _allIds = allIds ++ v.map(_._1)
        (_allIds, v.filterNot { case (id, _) => allIds.contains(id) } :: acc)
    }

    LimitList(acc)
  }

  implicit val actorSystem: ActorSystem = ActorSystem("yggdrasil_ops_spec")
  implicit def asyncContext = ExecutionContext.defaultExecutionContext
  val timeout = intToDurationInt(30).seconds

  object yggConfig extends YggConfig {
    def sortBufferSize = 6
    def sortWorkDir = new java.io.File("/tmp")
    def flatMapTimeout = timeout
  }

  implicit val chunkSerialization = BinaryProjectionSerialization
  val memoizationContext = MemoizationContext.Noop
  object ops extends Ops

  implicit val keyOrder: Order[ops.Key] = new Order[ops.Key] {
    def order(k1: ops.Key, k2: ops.Key) = (k1.size ?|? k2.size) |+| (k1 zip k2).foldLeft[Ordering](EQ) {
      case (ord, (v1, v2)) => ord |+| (v1 ?|? v2)
    }
  }

  def die(x: => Ops#X) = throw x

  "sort" should {
    "sort values" in check {
      (ll: LimitList[Vector[SEvent]]) => {
        val events = ll.values
        val enumP = enumPStream[Ops#X, Vector[SEvent], IO](events.toStream)
        val sorted = Await.result(ops.sort(DatasetEnum(Future(enumP)), None).fenum, timeout)
        val result = (consume[Ops#X, Vector[SEvent], IO, List] &= sorted[IO]).run(die _).unsafePerformIO.flatten 
        
        result must_== events.flatten.sorted
      }
    }
  }

  "group" should {
    "group values" in {
      implicit val dummyFS = FileSerialization.noop[Vector[(ops.Key, SEvent)]] 
      val enumP = enumPStream[Ops#X, Vector[SEvent], IO](Stream(Vector(SEvent(VectorCase(), SString("2")), SEvent(VectorCase(), SString("3"))), Vector(SEvent(VectorCase(), SString("1")))))
      val keyf: SEvent => List[SValue] = { ev => 
        ev._2.mapStringOr(List(SInt(0)))(s => List(SInt(s.toInt % 2)))
      }

      val grouped = Await.result((ops.group(DatasetEnum(Future(enumP), None), 0, BufferingContext.memory(100))(keyf)), intToDurationInt(30).seconds)

      val groups = (consume[Ops#X, (ops.Key, DatasetEnum[Ops#X, SEvent, IO]), IO, List] &= grouped[IO]).run(die _).unsafePerformIO
      groups must haveSize(2)
      groups(0) must beLike {
        case (List(SLong(0)), enum) => (consume[Ops#X, Vector[SEvent], IO, List] &= Await.result(enum.fenum, timeout).apply[IO]).run(die _).unsafePerformIO.flatten.flatMap(_._2.asString) must_== List("2")
      }

      groups(1) must beLike {
        case (List(SLong(1)), enum) => (consume[Ops#X, Vector[SEvent], IO, List] &= Await.result(enum.fenum, timeout).apply[IO]).run(die _).unsafePerformIO.flatten.flatMap(_._2.asString) must_== List("3", "1")
      }
    }

    "group arbitrary values" in todo
    // scalacheck: generate an arbitrary list of events where each value is from a single small distribution.
    // use .groupBy to determine a grouping, and ensure that the grouped enumerators when run produce the
    // same distribution
  }
}

// vim: set ts=4 sw=4 et:
