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
package com.precog.daze

import org.specs2.mutable._

import com.precog.yggdrasil._
import com.precog.common.Path
import scalaz._
import scalaz.std.list._

import com.precog.util.IdGen

import org.joda.time._
import org.joda.time.format._

trait TimeComparisonSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with LongIdMemoryDatasetConsumer[M] { self =>
      
  import Function._
  
  import dag._
  import instructions._
  import library._

  val testAPIKey = "testAPIKey"

  val line = Line(1, 1, "")

  def parseDateTime(time: String, fmt: String) = {
    Join(BuiltInFunction2Op(ParseDateTime), CrossLeftSort,
      Const(CString(time))(line),
      Const(CString(fmt))(line))(line)
  }

  def basicComparison(input: DepGraph, expected: Boolean) = {
    val result = testEval(input)
    
    result must haveSize(1)
    
    result must haveAllElementsLike {
      case (ids, SBoolean(d)) =>
        ids must haveSize(0)
        d mustEqual(expected)
    }
  }

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(testAPIKey, graph, Path.Root) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "comparison of two DateTimes of value provenance" should {
    "compute lt resulting in false" in {
      val input = Join(Lt, CrossLeftSort,
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jul 8, 1999 3:19:33 PM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, false)
    }

    "compute lt resulting in true" in {
      val input = Join(Lt, CrossLeftSort,
        parseDateTime("Jul 8, 1999 3:19:33 PM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, true)
    }

    "compute gt resulting in false" in {
      val input = Join(Gt, CrossLeftSort,
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, false)
    }

    "compute gt resulting in true" in {
      val input = Join(Gt, CrossLeftSort,
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jul 8, 1999 3:19:33 PM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, true)
    }

    "compute lteq resulting in false" in {
      val input = Join(LtEq, CrossLeftSort,
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jul 8, 1999 3:19:33 PM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, false)
    }

    "compute lteq resulting in true" in {
      val input = Join(LtEq, CrossLeftSort,
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, true)
    }

    "compute gteq resulting in false" in {
      val input = Join(GtEq, CrossLeftSort,
        parseDateTime("Jul 8, 1999 3:19:33 PM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, false)
    }

    "compute gteq resulting in true" in {
      val input = Join(GtEq, CrossLeftSort,
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, true)
    }

    "compute eq resulting in false" in {
      val input = Join(Eq, CrossLeftSort,
        parseDateTime("Jul 8, 1999 3:19:33 PM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, false)
    }

    "compute eq resulting in true" in {
      val input = Join(Eq, CrossLeftSort,
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, true)
    }

    "compute noteq resulting in false" in {
      val input = Join(NotEq, CrossLeftSort,
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, false)
    }

    "compute noteq resulting in true" in {
      val input = Join(NotEq, CrossLeftSort,
        parseDateTime("Jul 8, 1999 3:19:33 PM", "MMM d, yyyy h:mm:ss a"),
        parseDateTime("Jun 3, 2020 3:12:33 AM", "MMM d, yyyy h:mm:ss a"))(line)

      basicComparison(input, true)
    }
  }
}

object TimeComparisonSpecs extends TimeComparisonSpecs[test.YId] with test.YIdInstances
