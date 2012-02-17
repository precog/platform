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
import scalaz.std.AllInstances._
import Iteratee._
import MonadPartialOrder._

class YggdrasilEnumOpsComponentSpec extends Specification with YggdrasilEnumOpsComponent with Logging {
  type MemoContext = MemoizationContext.Noop.type
  type YggConfig = YggEnumOpsConfig

  implicit val actorSystem: ActorSystem = ActorSystem("yggdrasil_ops_spec")
  implicit def asyncContext = ExecutionContext.defaultExecutionContext

  object yggConfig extends YggConfig {
    def sortBufferSize = 10
    def sortWorkDir = sys.error("not used")
    def sortSerialization = SimpleProjectionSerialization
    def flatMapTimeout = intToDurationInt(30).seconds
  }

  val memoizationContext = MemoizationContext.Noop
  object ops extends Ops

  "sort" should {
    "sort values" in {
      implicit val SEventOrder: Order[SEvent] = Order[String].contramap((_: SEvent)._2.mapStringOr("")(a => a))
      val enumP = enumPStream[Unit, SEvent, IO](Stream(SEvent(Vector(), SString("2")), SEvent(Vector(), SString("3")), SEvent(Vector(), SString("1"))))
      val sorted = Await.result(ops.sort(DatasetEnum(Future(enumP)), None).fenum, intToDurationInt(30).seconds)

      (consume[Unit, SEvent, IO, List] &= sorted[IO])
      .run(_ => sys.error("...")).unsafePerformIO.map(_._2.mapStringOr("wrong")(a => a)) must_== List("1", "2", "3")
    }
  }
}

// vim: set ts=4 sw=4 et:
