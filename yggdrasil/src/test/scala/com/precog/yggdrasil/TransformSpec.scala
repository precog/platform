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

import org.specs2.mutable._

import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

trait TransformSpec extends TableModuleSpec {
  import trans._

  def checkTransformLeaf = check { (sample: SampleData) =>
    val table = fromJson(sample)
    val results = toJson(table.transform(Leaf(Source)))

    sample.data must_== results
  }

  def checkFilter = check { (sample: SampleData) =>
    val table = fromJson(sample)
    val results = toJson(table.transform {
      Filter(
        Leaf(Source), 
        Map1(Leaf(Source), liftF1({ case _ => CBoolean(true) })) 
      )
    })

    sample must_== results
  }
}

// vim: set ts=4 sw=4 et:
