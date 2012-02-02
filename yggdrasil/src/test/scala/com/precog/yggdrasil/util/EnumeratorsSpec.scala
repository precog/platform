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
package util

import com.precog.util._

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
import scalaz.std.AllInstances._
import Iteratee._
import MonadPartialOrder._

class EnumeratorsSpec extends Specification with ThrownMessages with Logging {
  "sort" should {
    "sort values" in {
      implicit val SEventOrder: Order[SEvent] = Order[String].contramap((_: SEvent)._2.mapStringOr("")(a => a))
      val enumP = enumPStream[Unit, SEvent, IO](Stream(SEvent(Vector(), SString("2")), SEvent(Vector(), SString("3")), SEvent(Vector(), SString("1"))))

      (consume[Unit, SEvent, IO, List] &= (Enumerators.sort(enumP, 5, null, null).apply[IO]))
      .run(_ => sys.error("...")).unsafePerformIO.map(_._2.mapStringOr("wrong")(a => a)) must_== List("1", "2", "3")
    }

/*
    "sort after zipWithIndex" in {
      val enum = EnumeratorP.enumPStream[Unit, SEvent, IO](Stream(
          SEvent(Vector(), SLong(1)), 
          SEvent(Vector(), SLong(5)), 
          SEvent(Vector(), SLong(3)), 
          SEvent(Vector(), SLong(2)), 
          SEvent(Vector(), SLong(7))
          ))

      val zipped = new EnumeratorP[Unit, (SEvent, Long), IO] {
        def apply[G[_]](implicit MO: G |>=| IO): EnumeratorT[Unit, (SEvent, Long), G] = {
          import MO._
          enum[G].zipWithIndex
        }
      }

      implicit val Ord: Order[(SEvent, Long)] = Order[Long].contramap((_: (SEvent, Long))._1._2.mapLongOr(sys.error("")){ a => a })

      (consume[Unit, (SEvent, Long), IO, List] &= Enumerators.sort[Unit](zipped, 10, null, null).apply[IO])
      .run(_ => sys.error("...")).unsafePerformIO.map(a => (a._1._2.mapLongOr(-1)(a => a), a._2)) must_== List((1L, 0L), (2L, 3L), (3L, 2L), (5L, 1L), (7L, 4L))
    }
    */
    
  }
}

// vim: set ts=4 sw=4 et:
