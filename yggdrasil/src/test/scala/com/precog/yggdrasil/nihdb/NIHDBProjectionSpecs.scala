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
package nihdb

import com.precog.common.ingest._
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
      val results = projection.getBlockAfter(None, None)

      results must whenDelivered(beNone)
    }

    "Insert and retrieve values below the cook threshold" in new TempContext {
      val expected: Seq[JValue] = Seq(JNum(0L), JNum(1L), JNum(2L))

      val toInsert = (0L to 2L).toSeq.map { i =>
        IngestRecord(EventId.fromLong(i), JNum(i))
      }

      val results =
        for {
          _ <- projection.insert(toInsert, "fake")
          result <- projection.getBlockAfter(None, None)
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

      projection.insert((0L to 2L).toSeq.map { i =>
        IngestRecord(EventId.fromLong(i), JNum(i))
      }, "fake")

      println("Raw log = " + fromFuture(projection.status).rawSize)

      val result = for {
        _ <- projection.close()
        _ <- Future(projection = newProjection(workDir))(actorSystem.dispatcher)
        status <- projection.status
        _ = println("Raw log = " + status.rawSize)
        r <- projection.getBlockAfter(None, None)
      } yield r

      result must whenDelivered (beLike {
        case Some(BlockProjectionData(min, max, data)) =>
          min mustEqual 0L
          max mustEqual 0L
          data.size mustEqual 3
          data.toJsonElements must containAllOf(expected).only.inOrder
      })
    }

    "Properly filter on constrainted columns" in todo

    "Properly convert raw blocks to cooked" in new TempContext {
      val expected: Seq[JValue] = (0L to 1950L).map(JNum(_)).toSeq

      (0L to 1950L).map {
        i => IngestRecord(EventId.fromLong(i), JNum(i))
      }.grouped(400).zipWithIndex.foreach { case (values, id) => projection.insert(values, "fake") }

      var waits = 10

      while (waits > 0 && fromFuture(projection.status).pending > 0) {
        Thread.sleep(200)
        waits -= 1
      }

      val status = fromFuture(projection.status)

      status.cooked mustEqual 1
      status.pending mustEqual 0
      status.rawSize mustEqual 751

      projection.getBlockAfter(None, None) must whenDelivered (beLike {
        case Some(BlockProjectionData(min, max, data)) =>
          min mustEqual 0L
          max mustEqual 0L
          data.size mustEqual 1200
          data.toJsonElements must containAllOf(expected.take(1200)).only.inOrder
      })

      projection.getBlockAfter(Some(0), None) must whenDelivered (beLike {
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
