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

import com.precog.common.VectorCase

class YggdrasilEnumOpsComponentSpec extends Specification with YggdrasilEnumOpsComponent with Logging {
  type MemoContext = MemoizationContext
  type YggConfig = YggEnumOpsConfig

  implicit val actorSystem: ActorSystem = ActorSystem("yggdrasil_ops_spec")
  implicit def asyncContext = ExecutionContext.defaultExecutionContext
  val timeout = intToDurationInt(30).seconds

  object yggConfig extends YggConfig {
    def sortBufferSize = 10
    def sortWorkDir = sys.error("not used")
    def flatMapTimeout = timeout
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
      val enumP = enumPStream[Unit, Vector[SEvent], IO](Stream(Vector(SEvent(VectorCase(), SString("2")), SEvent(VectorCase(), SString("3"))), Vector(SEvent(VectorCase(), SString("1")))))
      val sorted = Await.result(ops.sort(DatasetEnum(Future(enumP)), None).fenum, timeout)

      (consume[Unit, Vector[SEvent], IO, List] &= sorted[IO]).runOrZero.unsafePerformIO.flatten.flatMap(_._2.asString) must_== List("1", "2", "3")
    }
  }

  "group" should {
    "group values" in {
      implicit val dummyFS = FileSerialization.noop[Vector[(ops.Key, SEvent)]] 
      val enumP = enumPStream[Unit, Vector[SEvent], IO](Stream(Vector(SEvent(VectorCase(), SString("2")), SEvent(VectorCase(), SString("3"))), Vector(SEvent(VectorCase(), SString("1")))))
      val keyf: SEvent => List[SValue] = { ev => 
        ev._2.mapStringOr(List(SInt(0)))(s => List(SInt(s.toInt % 2)))
      }

      val grouped = Await.result((ops.group(DatasetEnum(Future(enumP), None), BufferingContext.memory(100))(keyf)), intToDurationInt(30).seconds)

      val groups = (consume[Unit, (ops.Key, DatasetEnum[Unit, SEvent, IO]), IO, List] &= grouped[IO]).runOrZero.unsafePerformIO
      groups must haveSize(2)
      groups(0) must beLike {
        case (List(SLong(0)), enum) => (consume[Unit, Vector[SEvent], IO, List] &= Await.result(enum.fenum, timeout).apply[IO]).runOrZero.unsafePerformIO.flatten.flatMap(_._2.asString) must_== List("2")
      }

      groups(1) must beLike {
        case (List(SLong(1)), enum) => (consume[Unit, Vector[SEvent], IO, List] &= Await.result(enum.fenum, timeout).apply[IO]).runOrZero.unsafePerformIO.flatten.flatMap(_._2.asString) must_== List("3", "1")
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
