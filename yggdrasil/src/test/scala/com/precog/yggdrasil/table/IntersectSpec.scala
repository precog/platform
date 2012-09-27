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
package table

import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser.parse
import blueeyes.json.JPathField

import scalaz._
import scalaz.syntax.bind._
import scalaz.syntax.copointed._

import org.specs2.ScalaCheck
import org.specs2.mutable._

import SampleData._

trait IntersectSpec[M[+_]] extends BlockStoreTestSupport[M] with Specification with ScalaCheck {
  implicit def M: Monad[M] with Copointed[M]

  val module = BlockStoreTestModule.empty[M]
  
  def testIntersect(sample: SampleData) = {
    import module._
    import module.trans._
    import module.trans.constants._

    val lstream = sample.data.zipWithIndex collect { case (v, i) if i % 2 == 0 => v }
    val rstream = sample.data.zipWithIndex collect { case (v, i) if i % 3 == 0 => v }

    val expected = sample.data.zipWithIndex collect { case (v, i) if i % 2 == 0 && i % 3 == 0 => v }

    val finalResults = for {
      results     <- Table.intersect(SourceKey.Single, fromJson(lstream), fromJson(rstream))
      jsonResult  <- results.toJson
    } yield jsonResult

    val jsonResult = finalResults.copoint

    jsonResult must_== expected
  }

  def testSimpleIntersect = {
    val s1 = SampleData(Stream(toRecord(Array(1l), parse("""{"a":[]}""")), toRecord(Array(2l), parse("""{"b":[]}"""))))

    testIntersect(s1)
  }

  def checkIntersect = {
    implicit val gen = sample(objectSchema(_, 3))
    check { (sample: SampleData) => testIntersect(sample.sortBy(_ \ "key")) }
  }
}
    
