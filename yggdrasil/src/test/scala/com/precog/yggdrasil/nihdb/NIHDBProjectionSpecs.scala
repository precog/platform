package com.precog.yggdrasil
package nihdb

import com.precog.niflheim._
import com.precog.yggdrasil.table._
import com.precog.util.IOUtils

import akka.actor.{ActorSystem, Props}
import akka.dispatch.Await
import akka.util.Duration

import blueeyes.akka_testing.FutureMatchers
import blueeyes.bkka.FutureMonad
import blueeyes.json._

import org.specs2.mutable.{After, Specification}
import org.specs2.specification.{Fragments, Step}

class NIHDBProjectionSpecs extends Specification with FutureMatchers {
  val actorSystem = ActorSystem("NIHDBActorSystem")

  val chef = actorSystem.actorOf(Props(new Chef(VersionedSegmentFormat(Map(1 -> V1SegmentFormat)))))

  implicit val M = new FutureMonad(actorSystem.dispatcher)

  trait TempContext extends After {
    val workDir = IOUtils.createTmpDir("nihdbspecs").unsafePerformIO

    def after = IOUtils.recursiveDelete(workDir).unsafePerformIO
  }

  "NIHDBProjections" should {
    "Properly initialize and close" in new TempContext {
      val projection = new NIHDBProjection(workDir, null, chef, 1000, actorSystem, Duration(60, "seconds"))

      projection.close()
    }
  }

  "NIHDBProjections" should {
    "Insert and retrieve values below the cook threshold" in new TempContext {
      val projection = new NIHDBProjection(workDir, null, chef, 1000, actorSystem, Duration(60, "seconds"))

      val results =
        for {
          _ <- projection.insert(Array(0l), Seq(JNum(0l)))
          _ <- projection.insert(Array(1l), Seq(JNum(1l)))
          _ <- projection.insert(Array(2l), Seq(JNum(2l)))
          result <- projection.getBlockAfter(None)
        } yield result

      results must whenDelivered (beLike {
        case Some(BlockProjectionData(min, max, data)) =>
          min mustEqual 0l
          max mustEqual 0l
          data.size mustEqual 3
      })

      Await.result(projection.close(), Duration(50, "seconds"))
    }
  }

  def shutdown = actorSystem.shutdown()

  override def map(fs: => Fragments) = fs ^ Step(shutdown)
}
