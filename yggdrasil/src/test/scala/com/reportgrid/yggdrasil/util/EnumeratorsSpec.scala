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
package com.reportgrid.yggdrasil
package util

import com.reportgrid.util._

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
import Iteratee._
import MonadPartialOrder._

class EnumeratorsSpec extends Specification with ThrownMessages with Logging {
  "sort" should {
    "sort values" in {
      implicit val SEventOrder: Order[SEvent] = Order[String].contramap((_: SEvent)._2.mapStringOr("")(a => a))
      val enumP: EnumeratorP[Unit, SEvent, IO] = new EnumeratorP[Unit, SEvent, IO] {
        def apply[F[_]](implicit MO: F |>=| IO): EnumeratorT[Unit, SEvent, F] = {
          import MO._
          enumStream[Unit, SEvent, F](Stream(SEvent(Vector(), SString("2")), SEvent(Vector(), SString("3")), SEvent(Vector(), SString("1"))))
        }
      }

      (consume[Unit, SEvent, IO, List] &= (Enumerators.sort(enumP, 5, null, null).apply[IO]))
      .run(_ => sys.error("...")).unsafePerformIO.map(_._2.mapStringOr("wrong")(a => a)) must_== List("1", "2", "3")
    }
  }
}

// vim: set ts=4 sw=4 et:
