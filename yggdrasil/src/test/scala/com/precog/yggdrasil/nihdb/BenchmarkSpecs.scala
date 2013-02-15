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

import com.precog.niflheim._
import com.precog.yggdrasil.table._
import com.precog.util.IOUtils

import akka.actor.{ActorSystem, Props}
import akka.dispatch.{Await, Future}
import akka.routing._
import akka.util.Duration

import blueeyes.akka_testing.FutureMatchers
import blueeyes.bkka.FutureMonad
import blueeyes.json._

import org.specs2.mutable.{After, Specification}
import org.specs2.specification.{Fragments, Step}

import scalaz.effect.IO

import java.io.File

class BenchmarkSpecs extends Specification with FutureMatchers {
  val actorSystem = ActorSystem("NIHDBActorSystem")

    val chefs = (1 to 4).map { _ => actorSystem.actorOf(Props(new Chef(
    VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)),
    VersionedSegmentFormat(Map(1 -> V1SegmentFormat)))
  )) }

  val chef = actorSystem.actorOf(Props[Chef].withRouter(RoundRobinRouter(chefs)))

  def newNihProjection(workDir: File, threshold: Int = 1000) = new NIHDBProjection(workDir, null, chef, threshold, actorSystem, Duration(60, "seconds"))

  implicit val M = new FutureMonad(actorSystem.dispatcher)

  trait TempContext extends After {
    val workDir = IOUtils.createTmpDir("nihdbspecs").unsafePerformIO
    var projection = newNihProjection(workDir)

    def fromFuture[A](f: Future[A]): A = Await.result(f, Duration(60, "seconds"))

    def close(proj: NIHDBProjection) = fromFuture(proj.close())

    def after = {
      (for {
        _ <- IO { close(projection) }
        _ <- IOUtils.recursiveDelete(workDir)
      } yield ()).unsafePerformIO
    }

    def runJDBM(f: File, sliceSize: Int, _eventid: Long): Long = {
      sys.error("TODO")
    }

    def runNIH(f: File, sliceSize: Int, _eventid: Long): Long = {
      var eventid: Long = _eventid

      println("start %s/%s" format (f, sliceSize))
      var t: Long = 0L

      def startit(): Unit = t = System.currentTimeMillis()

      def timeit(s: String) {
        val tt = System.currentTimeMillis()
        println("%s in %d ms" format (s, tt - t))
        t = tt
      }

      startit()

      val expected: Seq[JValue] = JParser.parseManyFromFile(f).valueOr(throw _)
      timeit("finished parsing")
  
      expected.grouped(sliceSize).foreach {
        values => projection.insert(Array(eventid), values)
        eventid += 1L
      }
      timeit("finished inserting")
  
      while (fromFuture(projection.stats).pending > 0) {
        Thread.sleep(200)
      }
      timeit("finished cooking")

      import scalaz._
      val stream = StreamT.unfoldM[Future, Unit, Option[Long]](None) { key =>
        projection.getBlockAfter(key).map(_.map { case BlockProjectionData(_, maxKey, _) => ((), Some(maxKey)) })
      }
      Await.result(stream.length, 30.seconds)
      timeit("evaluated")
      println("done %s/%s" format (f, sliceSize))

      eventid
    }
  }

  "NIHDBProjections" should {
    "Properly convert raw blocks to cooked" in new TempContext {
      var eventid: Long = 1L
      for {
        name <- List(/*"z10k_nl.json", */"z100k_nl.json")
        size <- List(50000) // List(1000, 10000, 20000, 50000)
      } yield {
        eventid = runNIH(new File("yggdrasil/src/test/resources/%s" format name), size, eventid)
      }
      // try out runJDBM too...
    }
  }

  def shutdown = actorSystem.shutdown()

  override def map(fs: => Fragments) = fs ^ Step(shutdown)
}
