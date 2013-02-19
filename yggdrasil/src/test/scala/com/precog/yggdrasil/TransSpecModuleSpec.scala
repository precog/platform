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

import test._
import table._

import com.precog.common._

import org.specs2.mutable.Specification

trait TransSpecModuleSpec extends TransSpecModule with FNDummyModule with Specification {
  import trans._
  import CPath._

  "concatChildren" should {
    "transform a CPathTree into a TransSpec" in {
      val tree: CPathTree[Int] = RootNode(Seq(
        FieldNode(CPathField("bar"), 
          Seq(
            IndexNode(CPathIndex(0), Seq(LeafNode(4))),  
            IndexNode(CPathIndex(1), Seq(FieldNode(CPathField("baz"), Seq(LeafNode(6))))),  
            IndexNode(CPathIndex(2), Seq(LeafNode(2))))),  
        FieldNode(CPathField("foo"), Seq(LeafNode(0)))))

      val result = TransSpec.concatChildren(tree)

      def leafSpec(idx: Int) = DerefArrayStatic(Leaf(Source), CPathIndex(idx))

      val expected = InnerObjectConcat(
        WrapObject(
          InnerArrayConcat(
            InnerArrayConcat(
              WrapArray(DerefArrayStatic(Leaf(Source),CPathIndex(4))), 
              WrapArray(WrapObject(DerefArrayStatic(Leaf(Source),CPathIndex(6)),"baz"))), 
            WrapArray(DerefArrayStatic(Leaf(Source),CPathIndex(2)))),"bar"), 
          WrapObject(
            DerefArrayStatic(Leaf(Source),CPathIndex(0)),"foo"))

      result mustEqual expected
    }
  }
}

object TransSpecModuleSpec extends TransSpecModuleSpec
