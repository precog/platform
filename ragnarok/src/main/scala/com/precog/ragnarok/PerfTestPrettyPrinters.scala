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

  private def prettyStats(stats: Option[Statistics], unit: String): String =
    stats map { s => 
      "%.1f ms  (s = %.1f %s)" format (s.mean, s.stdDev, unit)
    } getOrElse ""
    
  def toPrettyString(unit: String = "ms"): String = {
    def lines(test: Tree[(PerfTest, Option[Statistics])]): List[String] = {
      test match {
        case Tree.Node((Group(name), _), kids) =>
          name :: (kids.toList flatMap (lines(_)))

        case Tree.Node((RunSequential, s), kids) =>
          (kids.toList map (lines(_)) flatMap {
            case head :: tail =>
              (" + " + head) :: (tail map (" | " + _))
            case Nil => Nil
          }) ++ List(" ' " + prettyStats(s, unit), "")

        case Tree.Node((RunConcurrent, s), kids) =>
          (kids.toList map (lines(_)) flatMap {
            case head :: tail =>
              (" * " + head) :: (tail map (" | " + _))
            case Nil => Nil
          }) ++ List(" ' " + prettyStats(s, unit), "")

        case Tree.Node((RunQuery(q), s), kids) =>
          (q split "\n").toList match {
            case Nil => Nil
            case head :: tail =>
              ("-> " + head) :: (tail.foldRight(List(" ' " + prettyStats(s, unit), "")) {
                " | " + _ :: _
              })
          }
      }
    }

    lines(result) mkString "\n"
  }


  def toTsv: String = {
    def escape(s: String) = s.replace("\t", "    ")

    def lines(path: Option[String], test: Tree[(PerfTest, Option[Statistics])]): List[List[String]] =
      test match {
        case Tree.Node((RunQuery(query), Some(stats)), kids) =>
          val row = List(path getOrElse "", query,
            stats.mean.toString,
            stats.variance.toString,
            stats.stdDev.toString,
            stats.min.toString,
            stats.max.toString)
          
          row :: Nil

        case Tree.Node((Group(name), Some(stats)), kids) =>
          val newPath = path map (_ + ":" + escape(name)) getOrElse escape(name)

          val row = List(newPath, "",
            stats.mean.toString,
            stats.variance.toString,
            stats.stdDev.toString,
            stats.min.toString,
            stats.max.toString)

          row :: (kids.toList flatMap (lines(Some(newPath), _)))

        case Tree.Node(_, kids) =>
          kids.toList flatMap (lines(path, _))
      }

    lines(None, result) map (_ mkString "\t") mkString "\n"
  }


  def toFlatJson: String = {
    def escape(s: String) = s.replace("\\", "\\\\").replace("\"", "\\\"")

    def lines(path: Option[String], test: Tree[(PerfTest, Option[Statistics])]): List[String] =
      test match {
        case Tree.Node((Group(name), Some(stats)), kids) =>
          val newPath = path map (_ + ":" + escape(name)) getOrElse escape(name)
          val line = """"%s": {
            |  "mean": %f,
            |  "variance": %f,
            |  "stdDev": %f,
            |  "min": %f,
            |  "max": %f
            |}""" format (newPath, stats.mean, stats.variance, stats.stdDev, stats.min, stats.max)

          line :: (kids.toList flatMap (lines(Some(newPath), _)))

        case Tree.Node(_, kids) =>
          kids.toList flatMap (lines(path, _))
      }

    lines(None, result) mkString ("{", ",", "}")
  }
}

object PerfTestPrettyPrinters {
  implicit def statsPrinter(result: Tree[(PerfTest, Option[Statistics])]) =
    new PerfTestStatsPrettyPrinter(result)
}



