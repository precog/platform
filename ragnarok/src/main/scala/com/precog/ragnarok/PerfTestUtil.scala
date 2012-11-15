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
package com.precog
package ragnarok

import java.io.File

import scalaz._
import scalaz.std.option._

import akka.util.Duration

import akka.actor.ActorSystem
import akka.dispatch.{ Future, ExecutionContext, Await }
import blueeyes.bkka.AkkaTypeClasses._
import PerfTestPrettyPrinters._
import RunConfig.OutputFormat


class PerfTestUtil(rootDir: File, runs: Int = 30) {
  val config = RunConfig(rootDir = Some(rootDir), runs = runs)

  val timeout = Duration(config.queryTimeout, "seconds")

  implicit def futureIsCopointed(implicit ctx: ExecutionContext): Copointed[Future] = new Copointed[Future] {
    def map[A, B](m: Future[A])(f: A => B) = m map f
    def copoint[A](f: Future[A]) = Await.result(f, timeout)
  }

  def withRunner[A](config: RunConfig)(f: ExecutionContext => JDBMPerfTestRunner[Long] => A): A = {
    val actorSystem = ActorSystem("perfTestUtil")
    val result = try {

      implicit val execContext = ExecutionContext.defaultExecutionContext(actorSystem)

      val runner = new JDBMPerfTestRunner(SimpleTimer,
        optimize = config.optimize,
        apiKey = "dummyAPIKey",
        actorSystem = actorSystem,
        _rootDir = config.rootDir)

      runner.startup()
      try {
        f(execContext)(runner)
      } finally {
        runner.shutdown()
      }

    } finally {
      actorSystem.shutdown()
    }

    result
  }

  def ingest(path: String, file: File): Unit = withRunner(config) { implicit ctx => { runner =>
    runner.ingest(path, file).unsafePerformIO
  } }

  def test(query: String): String = withRunner(config) { implicit ctx => { runner =>
    val tails = (runs * (config.outliers / 2)).toInt
    val test = Tree.leaf[PerfTest](RunQuery(query))
    val result = runner.runAll(test, config.runs) {
      case None => None
      case Some((a, b)) =>
        Some(Statistics(MetricSpace[Long].distance(a, b), tails = tails))
    } map {
      case (t, stats) => (t, stats map (_ * (1 / 1000000.0))) // Convert to ms.
    }

    // result.toJson.toString
    result.toPrettyString
  } }
}

