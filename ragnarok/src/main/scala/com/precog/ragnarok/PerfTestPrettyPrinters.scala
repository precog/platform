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


final class PerfTestStatsPrettyPrinter(result: Tree[(PerfTest, Option[Statistics])]) {

  private def prettyStats(stats: Option[Statistics], toMillis: Double => Double): String =
    stats map { s => "%.1f ms" format toMillis(s.mean) } getOrElse ""
    
  def prettyStats(toMillis: Double => Double = identity): String = {
    def lines(test: Tree[(PerfTest, Option[Statistics])]): List[String] = {
      test match {
        case Tree.Node((Group(name), _), kids) =>
          name :: (kids.toList flatMap (lines(_)))

        case Tree.Node((RunSequential, s), kids) =>
          (kids.toList map (lines(_)) flatMap {
            case head :: tail =>
              (" + " + head) :: (tail map (" | " + _))
            case Nil => Nil
          }) :+ (" ` " + prettyStats(s, toMillis))

        case Tree.Node((RunConcurrent, s), kids) =>
          (kids.toList map (lines(_)) flatMap {
            case head :: tail =>
              (" * " + head) :: (tail map (" | " + _))
            case Nil => Nil
          }) :+ (" ` " + prettyStats(s, toMillis))

        case Tree.Node((RunQuery(q), s), kids) =>
          ("-> " + q) :: ("   " + prettyStats(s, toMillis)) :: Nil
      }
    }

    lines(result) mkString "\n"
  }
}

object PerfTestPrettyPrinters {
  implicit def statsPrinter(result: Tree[(PerfTest, Option[Statistics])]) =
    new PerfTestStatsPrettyPrinter(result)
}



