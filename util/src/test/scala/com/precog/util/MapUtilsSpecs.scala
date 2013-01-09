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
package com.precog.util

import org.specs2.ScalaCheck
import org.specs2.mutable._
import org.scalacheck._

import scalaz.Either3

object MapUtilsSpecs extends Specification with ScalaCheck with MapUtils {
  import Prop._
  
  "cogroup" should {
    "produce left, right and middle cases" in check { (left: Map[Int, List[Int]], right: Map[Int, List[Int]]) =>
      val result = left cogroup right
      
      val leftKeys = left.keySet -- right.keySet
      val rightKeys = right.keySet -- left.keySet
      val middleKeys = left.keySet & right.keySet
      
      val leftContrib = leftKeys.toSeq flatMap { key =>
        left(key) map { key -> Either3.left3[Int, (List[Int], List[Int]), Int](_) }
      }
      
      val rightContrib = rightKeys.toSeq flatMap { key =>
        right(key) map { key -> Either3.right3[Int, (List[Int], List[Int]), Int](_) }
      }
      
      val middleContrib = middleKeys.toSeq map { key =>
        key -> Either3.middle3[Int, (List[Int], List[Int]), Int]((left(key), right(key)))
      }
      
      result must containAllOf(leftContrib)
      result must containAllOf(rightContrib)
      result must containAllOf(middleContrib)
      result must haveSize(leftContrib.length + rightContrib.length + middleContrib.length)
    }
  }
}
