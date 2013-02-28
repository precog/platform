package com.precog.ragnarok

import scalaz._
import scalaz.std.option._

import akka.util.Duration

import java.io.File

import blueeyes.json._


object IngestTest {

  private def time[A](f: => Any): Long = {
    val start = System.nanoTime()
    val res = f
    System.nanoTime() - start
  }

  private def dataDirs(runner: NIHDBPerfTestRunner[_]): List[File] =
    List(runner.yggConfig.dataDir, runner.yggConfig.archiveDir,
      runner.yggConfig.cacheDir, runner.yggConfig.scratchDir)

  private def ensureDataDirsAreEmpty(runner: NIHDBPerfTestRunner[_]) {
    dataDirs(runner) foreach { dir =>
      if (!dir.exists()) {
        dir.mkdirs()
      } else if (!dir.list().isEmpty) {
        sys.error("Cannot run ingest performance tests on non-empty directory '%s'." format dir)
      }
    }
  }

  private def rm(f: File, exclusive: Boolean = false) {
    if (f.isDirectory()) f.listFiles() foreach (rm(_))
    if (!exclusive) f.delete()
  }

  private def deleteDataDirs(runner: NIHDBPerfTestRunner[_]) {
    dataDirs(runner) foreach (rm(_, true))
  }

  def run(config: RunConfig) {
    import akka.actor.ActorSystem
    import akka.dispatch.{ Future, ExecutionContext, Await }
    import PerfTestPrettyPrinters._
    import RunConfig.OutputFormat

    try {
      val runner = new NIHDBPerfTestRunner(SimpleTimer,
        optimize = config.optimize,
        apiKey = "dummyAPIKey",
        _rootDir = config.rootDir)

      runner.startup()
      try {

        ensureDataDirsAreEmpty(runner)

        ////////

        def timeIngest(path: String, file: File): Statistics = {
          val t = time(runner.ingest(path, file).unsafePerformIO)
          deleteDataDirs(runner)
          Statistics(t.toDouble / 1000000.0)
        }

        def run(n: Int): Map[String, Option[Statistics]] = {
          import scalaz.syntax.monoid._

          (0 until n).foldLeft(Map.empty[String, Option[Statistics]]) { (stats, _) =>
            config.ingest.foldLeft(stats) { case (stats, (path, file)) =>
              stats + (path -> (stats.getOrElse(path, None) |+| Some(timeIngest(path, file))))
            }
          }
        }

        run(config.dryRuns)
        val stats = run(config.runs)
        println(JObject(stats.toList collect { case (path, Some(s)) =>
          JField(path, s.toJson)
        }))

      } finally {
        runner.shutdown()
      }
    }
  }

  def main(args: Array[String]) {
    RunConfig.fromCommandLine(args.toList) match {
      case Failure(errors) =>
        System.err.println("Error parsing command lines:")
        errors.list foreach { msg => System.err.println("\t" + msg) }
        System.err.println()

      case Success(config) =>
        run(config)
    }
  }
}

