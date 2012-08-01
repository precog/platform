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
package com.precog.ragnarok

import scalaz._
import scalaz.syntax.applicative._
import scalaz.syntax.validation._


case class RunConfig(
    json: Boolean = true, // TODO do something with this.
    select: Option[(List[String], PerfTest) => Boolean] = None,
    runs: Int = 60,
    outliers: Double = 0.05,
    dryRuns: Int = 1) {
  def tails: Int = (runs * (outliers / 2)).toInt
}


object RunConfig {

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


  def fromCommandLine(args: List[String], config: ValidationNEL[String, RunConfig] = RunConfig().successNel): ValidationNEL[String, RunConfig] = args match {
    case Nil =>
      config

    case "--json" :: args =>
      fromCommandLine(args, config map (_.copy(json = true)))

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



