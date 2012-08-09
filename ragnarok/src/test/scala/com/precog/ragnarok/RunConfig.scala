package com.precog.ragnarok

import scala.annotation.tailrec

import scalaz._
import scalaz.syntax.applicative._
import scalaz.syntax.validation._


case class RunConfig(
    format: RunConfig.OutputFormat = RunConfig.OutputFormat.Legible,
    select: Option[(List[String], PerfTest) => Boolean] = None,
    runs: Int = 60,
    outliers: Double = 0.05,
    dryRuns: Int = 1,
    optimize: Boolean = true) {
  def tails: Int = (runs * (outliers / 2)).toInt
}


object RunConfig {

  sealed trait OutputFormat
  object OutputFormat {
    case object Json extends OutputFormat
    case object Tsv extends OutputFormat
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


  @tailrec
  def fromCommandLine(args: List[String], config: ValidationNEL[String, RunConfig] = RunConfig().successNel): ValidationNEL[String, RunConfig] = args match {
    case Nil =>
      config

    case "--json" :: args =>
      fromCommandLine(args, config map (_.copy(format = OutputFormat.Json)))

    case "--tsv" :: args =>
      fromCommandLine(args, config map (_.copy(format = OutputFormat.Tsv)))

    case "--no-optimize" :: args =>
      fromCommandLine(args, config map (_.copy(optimize = true)))

    case "--dry-runs" :: NonNegativeInt(runs) :: args =>
      fromCommandLine(args, config map (_.copy(dryRuns = runs.toInt)))

    case "--dry-runs" :: _ :: args =>
      fromCommandLine(args, config *> "The argument to --runs must be a positive integer".failNel)

    case "--runs" :: PositiveInt(runs) :: args =>
      fromCommandLine(args, config map (_.copy(runs = runs.toInt)))

    case "--runs" :: _ :: args =>
      fromCommandLine(args, config *> "The argument to --runs must be a positive integer".failNel)

    case "--outliers" :: OutlierPercentage(outliers) :: args =>
      fromCommandLine(args, config map (_.copy(outliers = outliers)))

    case "--outliers" :: _ :: args =>
      fromCommandLine(args, config *> "The argument to --outliers must be a real number in [0, 0.5)".failNel)

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



