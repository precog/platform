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

import scalaz._

import blueeyes.json._


trait JsonConverters {

  def perfTestToJson[A](result: Tree[(PerfTest, A)])(f: A => List[JField]): JValue = {
    def values(path: List[JString], test: Tree[(PerfTest, A)]): List[JValue] =
      test match {
        case Tree.Node((RunQuery(query), a), _) =>
          JObject(JField("path", JArray(path)) ::
            JField("query", JString(query)) :: f(a)) :: Nil

        case Tree.Node((Group(name), a), kids) =>
          val newPath = path :+ JString(name)
          val row = JObject(JField("path", JArray(newPath)) :: f(a))
          row :: kids.toList.flatMap(values(newPath, _))

        case Tree.Node(_, kids) =>
          kids.toList.flatMap(values(path, _))
      }

    JArray(values(Nil, result))
  }

  def perfTestDeltaToJson(result: Tree[(PerfTest, PerfDelta)]): JValue = {
    import PerfDelta._

    perfTestToJson(result) {
      case NoChange(baseline, stats) =>
        JField("baseline", baseline.toJson) ::
        JField("stats", stats.toJson) ::
        JField("delta", JString("insignificant")) :: Nil

      case Faster(baseline, stats) =>
        JField("baseline", baseline.toJson) ::
        JField("stats", stats.toJson) ::
        JField("delta", JString("faster")) :: Nil

      case Slower(baseline, stats) =>
        JField("baseline", baseline.toJson) ::
        JField("stats", stats.toJson) ::
        JField("delta", JString("slower")) :: Nil

      case MissingBaseline(stats) =>
        JField("stats", stats.toJson) :: Nil

      case MissingStats(baseline) =>
        JField("baseline", baseline.toJson) :: Nil

      case Missing =>
        Nil
    }
  }

  def perfTestResultToJson(result: Tree[(PerfTest, Option[Statistics])]): JValue = {
    perfTestToJson(result) {
      case Some(stats) => JField("stats", stats.toJson) :: Nil
      case None => Nil
    }
  }
}


trait PrettyPrinters {
  def prettyPerfTest[A](t: Tree[(PerfTest, A)])(prettyResult: A => String): String = {
    def lines(test: Tree[(PerfTest, A)]): List[String] = {
      test match {
        case Tree.Node((Group(name), _), kids) =>
          name :: (kids.toList flatMap (lines(_)))

        case Tree.Node((RunSequential, result), kids) =>
          (kids.toList map (lines(_)) flatMap {
            case head :: tail =>
              (" + " + head) :: (tail map (" | " + _))
            case Nil => Nil
          }) ++ List(" ' " + prettyResult(result), "")

        case Tree.Node((RunConcurrent, result), kids) =>
          (kids.toList map (lines(_)) flatMap {
            case head :: tail =>
              (" * " + head) :: (tail map (" | " + _))
            case Nil => Nil
          }) ++ List(" ' " + prettyResult(result), "")

        case Tree.Node((RunQuery(q), result), kids) =>
          (q split "\n").toList match {
            case Nil => Nil
            case head :: tail =>
              ("-> " + head) :: (tail.foldRight(List(" ' " + prettyResult(result), "")) {
                " | " + _ :: _
              })
          }
      }
    }

    lines(t) mkString "\n"
  }

  def prettyPerfTestDelta(test: Tree[(PerfTest, PerfDelta)]): String = {
    import PerfDelta._

    prettyPerfTest(test) {
      case NoChange(baseline, stats) =>
        "NO CHANGE  %.1f ms (s = %.1f ms)" format (stats.mean, stats.stdDev)
      case Faster(baseline, stats) =>
        "FASTER     %.1f ms (%.1 ms faster)" format (stats.mean, baseline.mean - stats.mean)
      case Slower(baseline, stats) =>
        "SLOWER     %.1f ms (%.1 ms slower)" format (stats.mean, stats.mean - baseline.mean)
      case MissingBaseline(stats) =>
        "TOTAL      %.1f ms" format stats.mean
      case MissingStats(_) | Missing =>
        ""
    }
  }

  def prettyPerfTestResult(result: Tree[(PerfTest, Option[Statistics])]): String =
    prettyPerfTest(result)(_ map { s =>
      "%.1f ms  (s = %.1f ms)" format (s.mean, s.stdDev)
    } getOrElse "")
}


object PerfTestPrettyPrinters extends PrettyPrinters with JsonConverters {
  implicit def statsPrinter(result: Tree[(PerfTest, Option[Statistics])]) =
    new PerfTestStatsPrettyPrinter(result)

  implicit def deltaPrinter(result: Tree[(PerfTest, PerfDelta)]) =
    new PerfTestDeltaPrettyPrinter(result)
}


final class PerfTestDeltaPrettyPrinter(result: Tree[(PerfTest, PerfDelta)]) extends PrettyPrinters with JsonConverters {

  def toJson: JValue = perfTestDeltaToJson(result)
  def toPrettyString: String = prettyPerfTestDelta(result)
}


final class PerfTestStatsPrettyPrinter(result: Tree[(PerfTest, Option[Statistics])]) extends PrettyPrinters with JsonConverters {

  def toJson: JValue = perfTestResultToJson(result)
  def toPrettyString: String = prettyPerfTestResult(result)
}


