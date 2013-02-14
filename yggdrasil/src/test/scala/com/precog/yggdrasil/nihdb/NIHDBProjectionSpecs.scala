package com.precog.yggdrasil
package nihdb

import com.precog.niflheim._
import com.precog.yggdrasil.table._
import com.precog.util.IOUtils

import akka.actor.{ActorSystem, Props}
import akka.dispatch.{Await, Future}
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

  val chef = actorSystem.actorOf(Props(new Chef(
    VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)),
    VersionedSegmentFormat(Map(1 -> V1SegmentFormat)))
  ))

  def newProjection(workDir: File, threshold: Int = 1000) = new NIHDBProjection(workDir, null, chef, threshold, actorSystem, Duration(60, "seconds"))

  implicit val M = new FutureMonad(actorSystem.dispatcher)

  trait TempContext extends After {
    val workDir = IOUtils.createTmpDir("nihdbspecs").unsafePerformIO
    var projection = newProjection(workDir)

    def fromFuture[A](f: Future[A]): A = Await.result(f, Duration(60, "seconds"))

    def close(proj: NIHDBProjection) = fromFuture(proj.close())

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
          _ <- projection.insert(Array(0L), Seq(JNum(0L)))
          _ <- projection.insert(Array(1L), Seq(JNum(1L)))
          _ <- projection.insert(Array(2L), Seq(JNum(2L)))
          result <- projection.getBlockAfter(None)
        } yield result

      results must whenDelivered (beLike {
        case Some(BlockProjectionData(min, max, data)) =>
          min mustEqual 0L
          max mustEqual 0L
          data.size mustEqual 3
          data.toJsonElements must containAllOf(expected).only.inOrder
      })
    }

    "Insert, close and re-read values below the cook threshold" in new TempContext {
      val expected: Seq[JValue] = Seq(JNum(0L), JNum(1L), JNum(2L))

      projection.insert(Array(0L), Seq(JNum(0L)))
      projection.insert(Array(1L), Seq(JNum(1L)))
      projection.insert(Array(2L), Seq(JNum(2L)))

      println("Raw log = " + fromFuture(projection.stats).rawSize)

      val result = for {
        _ <- projection.close()
        _ <- Future(projection = newProjection(workDir))(actorSystem.dispatcher)
        stats <- projection.stats
        _ = println("Raw log = " + stats.rawSize)
        r <- projection.getBlockAfter(None)
      } yield r

      result must whenDelivered (beLike {
        case Some(BlockProjectionData(min, max, data)) =>
          min mustEqual 0L
          max mustEqual 0L
          data.size mustEqual 3
          data.toJsonElements must containAllOf(expected).only.inOrder
      })
    }

    "Properly convert raw blocks to cooked" in new TempContext {
      val expected: Seq[JValue] = (0L to 1950L).map(JNum(_)).toSeq

      expected.grouped(400).zipWithIndex.foreach { case (values, id) => projection.insert(Array(id.toLong), values) }

      var waits = 10

      while (waits > 0 && fromFuture(projection.stats).pending > 0) {
        Thread.sleep(200)
        waits -= 1
      }

      val stats = fromFuture(projection.stats)

      stats.cooked mustEqual 1
      stats.pending mustEqual 0
      stats.rawSize mustEqual 751

      projection.getBlockAfter(None) must whenDelivered (beLike {
        case Some(BlockProjectionData(min, max, data)) =>
          min mustEqual 0L
          max mustEqual 0L
          data.size mustEqual 1200
          data.toJsonElements must containAllOf(expected.take(1200)).only.inOrder
      })

      projection.getBlockAfter(Some(0)) must whenDelivered (beLike {
        case Some(BlockProjectionData(min, max, data)) =>
          min mustEqual 1L
          max mustEqual 1L
          data.size mustEqual 751
          data.toJsonElements must containAllOf(expected.drop(1200)).only.inOrder
      })
    }
  }

  def shutdown = actorSystem.shutdown()

  override def map(fs: => Fragments) = fs ^ Step(shutdown)
}
