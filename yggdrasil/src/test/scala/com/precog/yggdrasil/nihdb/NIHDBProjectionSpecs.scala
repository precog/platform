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
import com.precog.common.security._
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
import org.specs2.ScalaCheck

import scalaz.effect.IO

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

class NIHDBProjectionSpecs extends Specification with ScalaCheck with FutureMatchers {
  val actorSystem = ActorSystem("NIHDBActorSystem")

  val chef = actorSystem.actorOf(Props(new Chef(
    VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)),
    VersionedSegmentFormat(Map(1 -> V1SegmentFormat)))
  ))

  def newProjection(workDir: File, threshold: Int = 1000) =
    NIHDB.create(chef, Authorities("test"), workDir, threshold, Duration(60, "seconds"))(actorSystem).unsafePerformIO.map { db =>
      new NIHDBActorProjection(db)(actorSystem.dispatcher)
    }.valueOr { e => throw new Exception(e.message) }

  implicit val M = new FutureMonad(actorSystem.dispatcher)

  val maxDuration = Duration(60, "seconds")

  implicit val params = set(minTestsOk -> 10)

  val baseDir = IOUtils.createTmpDir("nihdbspecs").unsafePerformIO

  val dirSeq = new AtomicInteger

  assert(baseDir.isDirectory && baseDir.canWrite)

  trait TempContext {
    val workDir = new File(baseDir, "nihdbspec%03d".format(dirSeq.getAndIncrement))

    if (!workDir.mkdirs) {
      throw new Exception("Failed to create work directory! " + workDir)
    }

    var projection = newProjection(workDir)

    def fromFuture[A](f: Future[A]): A = Await.result(f, Duration(60, "seconds"))

    def close(proj: NIHDBProjection) = fromFuture(proj.close(actorSystem))

    def stop = {
      (for {
        _ <- IO { close(projection) }
        _ <- IOUtils.recursiveDelete(workDir)
      } yield ()).unsafePerformIO
    }
  }

  "NIHDBProjections" should {
    "Properly initialize and close" in check { (discard: Int) =>
      val ctxt = new TempContext {}
      import ctxt._

      val results = projection.getBlockAfter(None, None)

      results.onComplete { _ => ctxt.stop } must awaited(maxDuration) { beNone }
    }

    "Insert and retrieve values below the cook threshold" in check { (discard: Int) =>
      val ctxt = new TempContext {}
      import ctxt._

      val expected: Seq[JValue] = Seq(JNum(0L), JNum(1L), JNum(2L))

      val toInsert = (0L to 2L).toSeq.map { i =>
        IngestRecord(EventId.fromLong(i), JNum(i))
      }

      val results =
        for {
          _ <- projection.insert(toInsert)
          result <- projection.getBlockAfter(None, None)
        } yield result

      results.onComplete { _ => ctxt.stop }  must awaited(maxDuration) (beLike {
        case Some(BlockProjectionData(min, max, data)) =>
          min mustEqual 0L
          max mustEqual 0L
          data.size mustEqual 3
          data.toJsonElements.map(_("value")) must containAllOf(expected).only.inOrder
      })
    }

    "Insert, close and re-read values below the cook threshold" in check { (discard: Int) =>
      val ctxt = new TempContext {}
      import ctxt._

      val expected: Seq[JValue] = Seq(JNum(0L), JNum(1L), JNum(2L))

      projection.insert((0L to 2L).toSeq.map { i =>
        IngestRecord(EventId.fromLong(i), JNum(i))
      })

      val result = for {
        _ <- projection.close(actorSystem)
        _ <- Future(projection = newProjection(workDir))(actorSystem.dispatcher)
        status <- projection.status
        r <- projection.getBlockAfter(None, None)
      } yield r

      result.onComplete { _ => ctxt.stop } must awaited(maxDuration) {
        beLike {
          case Some(BlockProjectionData(min, max, data)) =>
            min mustEqual 0L
            max mustEqual 0L
            data.size mustEqual 3
            data.toJsonElements.map(_("value")) must containAllOf(expected).only.inOrder
        }
      }
    }

    "Properly filter on constrainted columns" in todo

    "Properly convert raw blocks to cooked" in check { (discard: Int) =>
      val ctxt = new TempContext {}
      import ctxt._

      val expected: Seq[JValue] = (0L to 1950L).map(JNum(_)).toSeq

      (0L to 1950L).map {
        i => IngestRecord(EventId.fromLong(i), JNum(i))
      }.grouped(400).zipWithIndex.foreach { case (values, id) => projection.insert(values) }

      var waits = 10

      while (waits > 0 && fromFuture(projection.status).pending > 0) {
        Thread.sleep(200)
        waits -= 1
      }

      val status = fromFuture(projection.status)

      status.cooked mustEqual 1
      status.pending mustEqual 0
      status.rawSize mustEqual 751

      val result = for {
        firstBlock <- projection.getBlockAfter(None, None)
        secondBlock <- projection.getBlockAfter(Some(0), None)
      } yield {
        ctxt.stop
        (firstBlock, secondBlock)
      }

      result must awaited(maxDuration) (beLike {
        case (Some(BlockProjectionData(min1, max1, data1)), Some(BlockProjectionData(min2, max2, data2))) =>
          min1 mustEqual 0L
          max1 mustEqual 0L
          data1.size mustEqual 1200
          data1.toJsonElements.map(_("value")) must containAllOf(expected.take(1200)).only.inOrder

          min2 mustEqual 1L
          max2 mustEqual 1L
          data2.size mustEqual 751
          data2.toJsonElements.map(_("value")) must containAllOf(expected.drop(1200)).only.inOrder
      })
    }

  }

  def shutdown = {
    actorSystem.shutdown()
    IOUtils.recursiveDelete(baseDir).unsafePerformIO
    assert(!baseDir.isDirectory)
  }

  override def map(fs: => Fragments) = fs ^ Step(shutdown)
}
