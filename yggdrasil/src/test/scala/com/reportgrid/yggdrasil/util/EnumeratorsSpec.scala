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
