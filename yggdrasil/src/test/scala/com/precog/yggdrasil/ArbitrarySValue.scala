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
import org.scalacheck.Arbitrary
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary._

import scalaz.Order
import scalaz.std.list._
import scalaz.std.anyVal._

trait SValueGenerators {
  def svalue(depth: Int): Gen[SValue] = {
    if (depth <= 0) sleaf 
    else oneOf(1, 2, 3) flatMap { //it's much faster to lazily compute the subtrees
      case 1 => sobject(depth)
      case 2 => sarray(depth)
      case 3 => sleaf
    }
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
    alphaStr map (SString(_: String)),
    arbitrary[Boolean] map (SBoolean(_: Boolean)),
    arbitrary[Long]    map (l => SDecimal(BigDecimal(l))),
    arbitrary[Double]  map (d => SDecimal(BigDecimal(d))),
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

case class LimitList[A](values: List[A])

object LimitList {
  def genLimitList[A: Gen](size: Int): Gen[LimitList[A]] = for {
    i <- choose(0, size)
    l <- listOfN(i, implicitly[Gen[A]])
  } yield LimitList(l)
}

trait ArbitrarySValue extends SValueGenerators {
  def genChunks(size: Int): Gen[LimitList[Vector[SEvent]]] = LimitList.genLimitList[Vector[SEvent]](size)

  implicit val SEventIdentityOrder: Order[SEvent] = Order[List[Long]].contramap((_: SEvent)._1.toList)
  implicit val SEventOrdering = SEventIdentityOrder.toScalaOrdering

  implicit val SEventChunkGen: Gen[Vector[SEvent]] = chunk(3, 3, 2)
  implicit val ArbitraryChunks = Arbitrary(genChunks(5))
}


// vim: set ts=4 sw=4 et:
