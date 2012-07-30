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
package daze

import bytecode.RandomLibrary
import org.specs2.mutable._

object ReductionFinderSpecs extends Specification with ReductionFinder with RandomLibrary {
  import instructions._
  import dag._

  "reduction finder" should {
    "in a load, find no reductions when there aren't any" >> {
      val line = Line(0, "")

      val input = dag.LoadLocal(line, None, Root(line, PushString("/foo")), Het)
      val expected = Map.empty[DepGraph, Vector[Reduction]]

      findReductions(input) mustEqual expected
    }

    "in a single reduction" >> {
      val line = Line(0, "")

      val input = dag.Reduce(line, BuiltInReduction(BIR(Vector(), "count", 0x2000)), 
        dag.LoadLocal(line, None, Root(line, PushString("/foo")), Het))


      val parent = dag.LoadLocal(line, None, Root(line, PushString("/foo")), Het)
      val red = BuiltInReduction(BIR(Vector(), "count", 0x2000))
      val expected = Map(parent -> Vector(red))

      findReductions(input) mustEqual expected
    }   

    "in a join of two reductions on the same dataset" >> {
      val line = Line(0, "")

      val input = Join(line, Map2Cross(Add), 
        dag.Reduce(line, BuiltInReduction(BIR(Vector(), "count", 0x2000)), 
          dag.LoadLocal(line, None, Root(line, PushString("/foo")), Het)),
        dag.Reduce(line, BuiltInReduction(BIR(Vector(), "stdDev", 0x2007)), 
          dag.LoadLocal(line, None, Root(line, PushString("/foo")), Het)))


      val parent = dag.LoadLocal(line, None, Root(line, PushString("/foo")), Het)
      val red1 = BuiltInReduction(BIR(Vector(), "count", 0x2000))
      val red2 = BuiltInReduction(BIR(Vector(), "stdDev", 0x2007))
      val expected = Map(parent -> Vector(red1, red2))

      findReductions(input) mustEqual expected
