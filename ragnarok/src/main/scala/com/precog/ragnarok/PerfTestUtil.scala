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
        userUID = "dummy",
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

