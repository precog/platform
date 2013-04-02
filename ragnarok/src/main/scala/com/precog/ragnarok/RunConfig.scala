package com.precog.ragnarok

import java.io.File

import scala.annotation.tailrec

import scalaz._
import scalaz.syntax.applicative._
import scalaz.syntax.validation._


case class RunConfig(
    format: RunConfig.OutputFormat = RunConfig.OutputFormat.Legible,
    select: Option[(List[String], PerfTest) => Boolean] = None,
    runs: Int = 60,
    outliers: Double = 0.05,
    dryRuns: Int = 10,
    optimize: Boolean = true,
    baseline: Option[File] = None,
    rootDir: Option[File] = None,
    ingest: List[(String, File)] = Nil,
    queryTimeout: Int = 5 * 60) {
  def tails: Int = (runs * (outliers / 2)).toInt
}


object RunConfig {

  sealed trait OutputFormat
  object OutputFormat {
    case object Json extends OutputFormat
    case object Legible extends OutputFormat
  }


  implicit object semigroup extends Semigroup[RunConfig] {
    def append(a: RunConfig, b: => RunConfig) = b
  }


  val NonNegativeInt = """([0-9]+)""".r
  val PositiveInt = """([1-9][0-9]*)""".r

  object OutlierPercentage {
    def unapply(str: String): Option[Double] = try {
      val p = str.toDouble
      if (p >= 0.0 && p < 0.5) Some(p) else None
    } catch {
      case _: NumberFormatException =>
        None
    }
  }

  def fromCommandLine(args: Array[String]): ValidationNel[String, RunConfig] = fromCommandLine(args.toList)

  @tailrec
  def fromCommandLine(args: List[String], config: ValidationNel[String, RunConfig] = RunConfig().successNel): ValidationNel[String, RunConfig] = args match {
    case Nil =>
      config

    case "--baseline" :: file :: args =>
      val f = new File(file)
      if (f.isFile && f.canRead) {
        fromCommandLine(args, config map (_. copy(baseline = Some(f))))
      } else {
        fromCommandLine(args, config *> "The baseline file must be regular and readable.".failureNel)
      }

    case "--json" :: args =>
      fromCommandLine(args, config map (_.copy(format = OutputFormat.Json)))

    case "--no-optimize" :: args =>
      fromCommandLine(args, config map (_.copy(optimize = false)))

    case "--dry-runs" :: NonNegativeInt(runs) :: args =>
      fromCommandLine(args, config map (_.copy(dryRuns = runs.toInt)))

    case "--dry-runs" :: _ :: args =>
      fromCommandLine(args, config *> "The argument to --runs must be a positive integer".failureNel)

    case "--runs" :: PositiveInt(runs) :: args =>
      fromCommandLine(args, config map (_.copy(runs = runs.toInt)))

    case "--runs" :: _ :: args =>
      fromCommandLine(args, config *> "The argument to --runs must be a positive integer".failureNel)

    case "--outliers" :: OutlierPercentage(outliers) :: args =>
      fromCommandLine(args, config map (_.copy(outliers = outliers)))

    case "--outliers" :: _ :: args =>
      fromCommandLine(args, config *> "The argument to --outliers must be a real number in [0, 0.5)".failureNel)

    case "--root-dir" :: rootDir :: args =>
      fromCommandLine(args, config map (_.copy(rootDir = Some(new File(rootDir)))))

    case "--ingest" :: db :: file :: args =>
      fromCommandLine(args, config map { cfg =>
        cfg.copy(ingest = cfg.ingest :+ (db -> new File(file)))
      })

    case "--timeout" :: NonNegativeInt(to) :: args =>
      fromCommandLine(args, config map (_.copy(queryTimeout = to.toInt)))

    case "--timeout" :: _ :: args =>
      fromCommandLine(args, config *> "The argument to --timeout must be a non-negative number".failureNel)

    case test :: args =>
      fromCommandLine(args, config map { config =>
        val g = { (path: List[String], _: Any) => path contains test }
        val select = config.select map { f =>
          (path: List[String], test: PerfTest) => (f(path, test) || g(path, test))
        } orElse Some(g)

        config.copy(select = select)
      })
  }
}

