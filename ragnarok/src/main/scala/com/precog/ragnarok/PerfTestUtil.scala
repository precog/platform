package com.precog
package ragnarok

import java.io.File

import scalaz._
import scalaz.std.option._

import akka.util.Duration

import akka.actor.ActorSystem
import akka.dispatch.{ Future, ExecutionContext, Await }
import PerfTestPrettyPrinters._
import RunConfig.OutputFormat


class PerfTestUtil(rootDir: File, runs: Int = 30) {
  val config = RunConfig(rootDir = Some(rootDir), runs = runs)

  val timeout = Duration(config.queryTimeout, "seconds")

  def withRunner[A](config: RunConfig)(f: JDBMPerfTestRunner[Long] => A): A = {
    val result = try {
      val runner = new JDBMPerfTestRunner(SimpleTimer,
        optimize = config.optimize,
        apiKey = "dummyAPIKey",
        _rootDir = config.rootDir,
        testTimeout = timeout
      )

      runner.startup()
      try {
        f(runner)
      } finally {
        runner.shutdown()
      }
    }

    result
  }

  def ingest(path: String, file: File): Unit = withRunner(config) { runner =>
    runner.ingest(path, file).unsafePerformIO
  }

  def test(query: String): String = withRunner(config) { runner =>
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
  }
}

