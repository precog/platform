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
package com.precog.yggdrasil

import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import com.precog.common.VectorCase

import org.specs2.mutable.Specification
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary._

object Generators {
  //@volatile var svcount = 0

  def svalue(depth: Int): Gen[SValue] = {
    //synchronized { svcount += 1 }
    if (depth <= 0) sleaf else oneOf(sobject(depth), sarray(depth), sleaf)
  }

  def sobject(depth: Int): Gen[SValue] = {
    for {
      size <- choose(1, 3)
      names <- listOfN(size, identifier)
      values <- listOfN(size, svalue(depth - 1))  
    } yield {
      SObject((names zip values).toMap)
    }
  }

  def sarray(depth: Int): Gen[SValue] = {
    for {
      size <- choose(1, 3)
      l <- listOfN(size, svalue(depth - 1))
    } yield SArray(Vector(l: _*))
  }


  def sleaf: Gen[SValue] = oneOf(
    alphaStr map SString,
    arbitrary[Boolean] map SBoolean,
    arbitrary[Long] map SLong,
    arbitrary[Double] map SDouble,
    //arbitrary[BigDecimal] map SDecimal, //scalacheck's BigDecimal gen will overflow at random
    value(SNull)
  )

  def sevent(idCount: Int, vdepth: Int): Gen[SEvent] = {
    for {
      ids <- listOfN(idCount, arbitrary[Long])
      value <- svalue(vdepth)
    } yield (VectorCase(ids: _*), value)
  }

  def chunk(size: Int, idCount: Int, vdepth: Int): Gen[Vector[SEvent]] = 
    listOfN(size, sevent(idCount, vdepth)) map { l => Vector(l: _*) }
}

class SValueSpec extends Specification {
  "set" should {
    "set properties on an object" in {
      SObject(Map()).set(JPath(".foo.bar"), CString("baz")) must beSome(SObject(Map("foo" -> SObject(Map("bar" -> SString("baz"))))))
    }

    "set array indices" in {
      SObject(Map()).set(JPath(".foo[1].bar"), CString("baz")) must beSome(SObject(Map("foo" -> SArray(Vector(SNull, SObject(Map("bar" -> SString("baz"))))))))
    }

    "return None for a primitive" in {
      SLong(1).set(JPath(".foo.bar"), CString("hi")) must beNone
    }
  }

  "structure" should {
    "return correct sequence for an array" in {
      SArray(Vector(SBoolean(true))).structure must_== Seq((JPath("[0]"), SBoolean))
    }
  }
}

// vim: set ts=4 sw=4 et:
