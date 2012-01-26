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

class EnumeratorsSpec extends Specification with ThrownMessages with Logging {
  "sort" should {
    "sort values" in {
      implicit val bij: Bijection[String, (Array[Byte], Array[Byte])] = null
      val enumP: EnumeratorP[Unit, String, IO] = new EnumeratorP[Unit, String, IO] {
        def apply[F[_[_], _]: MonadTrans]: EnumeratorT[Unit, String, ({type λ[α] = F[IO, α]})#λ] = {
          type FIO[α] = F[IO, α]
          implicit val MF = MonadTrans[F].apply[IO]
          enumStream[Unit, String, FIO](Stream("2", "3", "1"))
        }
      }

      type IdIO[α] = IdT[IO, α]
      (consume[Unit, String, IdIO, List] &= (Enumerators.sort(enumP, 5, null, null).apply[IdT]))
      .run(_ => sys.error("...")).run.unsafePerformIO must_== List("1", "2", "3")
    }
  }
}

// vim: set ts=4 sw=4 et:
