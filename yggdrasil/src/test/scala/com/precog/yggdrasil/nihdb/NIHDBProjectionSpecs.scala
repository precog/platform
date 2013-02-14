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

import scalaz.effect.IO

import java.io.File


class NIHDBProjectionSpecs extends Specification with FutureMatchers {
  val actorSystem = ActorSystem("NIHDBActorSystem")

  val chef = actorSystem.actorOf(Props(new Chef(VersionedSegmentFormat(Map(1 -> V1SegmentFormat)))))

  def newProjection(workDir: File, threshold: Int = 1000) = new NIHDBProjection(workDir, null, chef, threshold, actorSystem, Duration(60, "seconds"))

  implicit val M = new FutureMonad(actorSystem.dispatcher)

  trait TempContext extends After {
    val workDir = IOUtils.createTmpDir("nihdbspecs").unsafePerformIO
    var projection = newProjection(workDir)

    def close(proj: NIHDBProjection) = Await.result(proj.close(), Duration(50, "seconds")) 

    def after = {
      (for {
        _ <- IO { close(projection) }
        _ <- IOUtils.recursiveDelete(workDir)
      } yield ()).unsafePerformIO
    }
  }

  "NIHDBProjections" should {
    "Properly initialize and close" in new TempContext {
      val results = projection.getBlockAfter(None)

      results must whenDelivered(beNone)
    }

    "Insert and retrieve values below the cook threshold" in new TempContext {
      val expected: Seq[JValue] = Seq(JNum(0L), JNum(1L), JNum(2L))

      val results =
        for {
          _ <- projection.insert(Array(0l), Seq(JNum(0L)))
          _ <- projection.insert(Array(1l), Seq(JNum(1L)))
          _ <- projection.insert(Array(2l), Seq(JNum(2L)))
          result <- projection.getBlockAfter(None)
        } yield result

      results must whenDelivered (beLike {
        case Some(BlockProjectionData(min, max, data)) =>
          min mustEqual 0l
          max mustEqual 0l
          data.size mustEqual 3
          data.toJsonElements must containAllOf(expected).only.inOrder
      })
    }

    "Insert, close and re-read values below the cook threshold" in new TempContext {
      val expected: Seq[JValue] = Seq(JNum(0L), JNum(1L), JNum(2L))

      projection.insert(Array(0l), Seq(JNum(0L)))
      projection.insert(Array(1l), Seq(JNum(1L)))
      projection.insert(Array(2l), Seq(JNum(2L)))

      val result = projection.close().flatMap { _ =>
        projection = newProjection(workDir)
        projection.getBlockAfter(None)
      }

      result must whenDelivered (beLike {
        case Some(BlockProjectionData(min, max, data)) =>
          min mustEqual 0l
          max mustEqual 0l
          data.size mustEqual 3
          data.toJsonElements must containAllOf(expected).only.inOrder
      })
    }
  }

  def shutdown = actorSystem.shutdown()

  override def map(fs: => Fragments) = fs ^ Step(shutdown)
}
