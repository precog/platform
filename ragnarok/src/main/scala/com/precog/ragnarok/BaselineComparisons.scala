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
import JsonAST.JValue


sealed trait PerfDelta
object PerfDelta {
  case class Faster(baseline: Statistics, stats: Statistics) extends PerfDelta
  case class Slower(baseline: Statistics, stats: Statistics) extends PerfDelta
  case class NoChange(baseline: Statistics, stats: Statistics) extends PerfDelta

  case class MissingBaseline(stats: Statistics) extends PerfDelta
  case class MissingStats(baseline: Statistics) extends PerfDelta

  case object Missing extends PerfDelta

  def apply(baseline: Option[Statistics], stats: Option[Statistics]): PerfDelta = (baseline, stats) match {
    case (Some(baseline), Some(stats)) =>
      val q1 = baseline.variance / baseline.count
      val q2 = stats.variance / stats.count
      val qsum = q1 + q2
      val t = math.abs((baseline.mean - stats.mean) / math.sqrt(qsum))

      // TODO Calculate DoF and use table lookup.
      // val df = (qsum * qsum) / ((q1 * q1) / (baseline.count - 1) + (q2 * q2) / (stats.count - 1))

      if (t > 2.0) {
        if (stats.mean > baseline.mean) {
          Slower(baseline, stats)
        } else {
          Faster(baseline, stats)
        }
      } else {
        NoChange(baseline, stats)
      }

    case (Some(baseline), None) =>
      MissingStats(baseline)

    case (None, Some(stats)) =>
      MissingBaseline(stats)

    case (None, None) =>
      Missing
  }
}


/**
 * Provides methods to read in a baseline JSON file.
 */
trait BaselineComparisons {
  import java.io.Reader
  import JsonAST._

  import scalaz.std.option._
  import scalaz.syntax.applicative._

  type TestPath = (List[String], Option[String])

  def compareWithBaseline(results: Tree[(PerfTest, Option[Statistics])], baseline: Map[TestPath, Statistics]): Tree[(PerfTest, PerfDelta)] = {
    def rec(results: Tree[(PerfTest, Option[Statistics])], path: List[String]): Tree[(PerfTest, PerfDelta)] =
      results match {
        case Tree.Node((RunQuery(query), stats), _) =>
          Tree.leaf(RunQuery(query) -> PerfDelta(baseline get ((path, Some(query))), stats))

        case Tree.Node((Group(name), stats), kids) =>
          val newPath = path :+ name
          Tree.node(Group(name) -> PerfDelta(baseline get ((newPath, None)), stats), kids map (rec(_, newPath)))

        case Tree.Node((test, stats), kids) =>
          Tree.node(test -> PerfDelta(None, stats), kids map (rec(_, path)))
      }

    rec(results, Nil)
  }

  def readBaseline(reader: Reader): Map[TestPath, Statistics] = {
    JsonParser.parse(reader) match {
      case JArray(tests) =>
        tests.foldLeft(Map[(List[String], Option[String]), Statistics]()) { case (acc, obj) =>
          (obj \? "stats") match {
            case Some(stats) =>
              (for {
                JArray(jpath) <- obj \? "path" flatMap (_ -->? classOf[JArray])
                JNum(mean) <- stats \? "mean" flatMap (_ -->? classOf[JNum])
                JNum(variance) <- stats \? "variance" flatMap (_ -->? classOf[JNum])
                JNum(stdDev) <- stats \? "stdDev" flatMap (_ -->? classOf[JNum])
                JNum(min) <- stats \? "min" flatMap (_ -->? classOf[JNum])
                JNum(max) <- stats \? "max" flatMap (_ -->? classOf[JNum])
                JNum(count) <- stats \? "count" flatMap (_ -->? classOf[JNum])
              } yield {
                val path = (jpath collect { case JString(p) => p },
                  (obj \? "query") collect { case JString(query) => query })
                val n = count.toInt
                path -> Statistics(0, List(min.toDouble), List(max.toDouble), mean.toDouble, (n - 1) * variance.toDouble, n)
              }) map (acc + _) getOrElse {
                // TODO: Replace these errors with something more useful.
                sys.error("Error parsing: %s" format obj.toString)
              }

            // Stats will be misssing when the Option[Statistics] is None.
            case None =>
              acc
          }
        }

      case _ =>
        sys.error("Top level JSON element in a baseline file must be an array.")
    }
  }


  def createBaseline(result: Tree[(PerfTest, Option[Statistics])]): JValue = {
    import JsonAST._

    def statsJson(stats: Statistics): List[JField] = List(
      JField("mean", JNum(stats.mean)),
      JField("variance", JNum(stats.variance)),
      JField("stdDev", JNum(stats.stdDev)),
      JField("min", JNum(stats.min)),
      JField("max", JNum(stats.max)),
      JField("count", JNum(stats.count)))
 
    def values(path: List[JString], test: Tree[(PerfTest, Option[Statistics])]): List[JValue] = {
      test match {
        case Tree.Node((RunQuery(query), Some(stats)), _) =>
          JObject(JField("path", JArray(path)) ::
            JField("query", JString(query)) :: statsJson(stats)) :: Nil

        case Tree.Node((Group(name), Some(stats)), kids) =>
          val newPath = path :+ JString(name)
          val row = JObject(JField("path", JArray(newPath)) :: statsJson(stats))
          row :: kids.toList.flatMap(values(newPath, _))

        case Tree.Node(_, kids) =>
          kids.toList.flatMap(values(path, _))
      }
    }

    JArray(values(Nil, result))
  }
}


object BaselineComparisons extends BaselineComparisons



