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
package com.precog.common

import org.specs2.mutable.Specification

class CPathSpec extends Specification {
  import CPath._

  "makeTree" should {
    "return correct tree given a sequence of CPath" in {
      val cpaths: Seq[CPath] = Seq(
        CPath(CPathField("foo")),
        CPath(CPathField("bar"), CPathIndex(0)),
        CPath(CPathField("bar"), CPathIndex(1), CPathField("baz")),
        CPath(CPathField("bar"), CPathIndex(1), CPathField("ack")),
        CPath(CPathField("bar"), CPathIndex(2)))

      val values: Seq[Int] = Seq(4, 6, 7, 2, 0)

      val result = makeTree(cpaths, values)

      val expected: CPathTree[Int] = {
        RootNode(Seq(
          FieldNode(CPathField("bar"), 
            Seq(
              IndexNode(CPathIndex(0), Seq(LeafNode(4))),  
              IndexNode(CPathIndex(1), 
                Seq(
                  FieldNode(CPathField("ack"), Seq(LeafNode(6))),  
                  FieldNode(CPathField("baz"), Seq(LeafNode(7))))),  
              IndexNode(CPathIndex(2), Seq(LeafNode(2))))),  
          FieldNode(CPathField("foo"), Seq(LeafNode(0)))))
      }

      result mustEqual expected
    }
  }
}
