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

import com.precog.common._
import com.precog.util._
import com.precog.yggdrasil.util._

import blueeyes.json._
import blueeyes.json.JsonAST._

import com.weiglewilczek.slf4s.Logging

import scala.annotation.tailrec
import scala.util.Random
import scalaz._
import scalaz.effect._
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.syntax.copointed._
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._

import org.specs2.ScalaCheck
import org.specs2.mutable._

import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import SampleData._

trait BlockAlignSpec[M[+_]] extends BlockStoreTestSupport[M] with Specification with ScalaCheck { self =>
  def testAlign(sample: SampleData) = {
    println("===========================================================================")
    object module extends BlockStoreTestModule {
      val projections = Map.empty[ProjectionDescriptor, Projection]
    }

    import module._
    import module.trans._
    import module.trans.constants._

    val lstream = sample.data.zipWithIndex collect { case (v, i) if i % 2 == 0 => v }
    val rstream = sample.data.zipWithIndex collect { case (v, i) if i % 3 == 0 => v }

    val expected = sample.data.zipWithIndex collect { case (v, i) if i % 2 == 0 && i % 3 == 0 => v }

    val finalResults = for {
      results <- Table.align(fromJson(lstream), SourceKey.Single, fromJson(rstream), SourceKey.Single)
      leftResult  <- results._1.toJson
      rightResult <- results._2.toJson
    } yield {
      (rightResult, leftResult)
    }

    val (rightResult, leftResult) = finalResults.copoint
    leftResult must_== expected
    rightResult must_== expected
  }

  def checkAlign = {
    implicit val gen = sample(objectSchema(_, 3))
    check { (sample: SampleData) => testAlign(sample.sortBy(_ \ "key")) }
  }

  def alignAcrossBoundaries = {
    val JArray(elements) = JsonParser.parse("""[
        {
          "value":{
            "fr8y":-2.761198250953116839E+14037,
            "hw":[],
            "q":2.429467767811669098E+50018
          },
          "key":[1.0,2.0]
        },
        {
          "value":{
            "fr8y":8862932465119160.0,
            "hw":[],
            "q":-7.06989214308545856E+34226
          },
          "key":[2.0,1.0]
        },
        {
          "value":{
            "fr8y":3.754645750547307163E-38452,
            "hw":[],
            "q":-2.097582685805979759E+29344
          },
          "key":[2.0,4.0]
        },
        {
          "value":{
            "fr8y":0.0,
            "hw":[],
            "q":2.839669248714535100E+14955
          },
          "key":[3.0,4.0]
        },
        {
          "value":{
            "fr8y":-1E+8908,
            "hw":[],
            "q":6.56825624988914593E-49983
          },
          "key":[4.0,2.0]
        },
        {
          "value":{
            "fr8y":123473018907070400.0,
            "hw":[],
            "q":0E+35485
          },
          "key":[4.0,4.0]
        }]
    """)

    val sample = SampleData(elements.toStream, Some((2,List((JPath(".q"),CNum), (JPath(".hw"),CEmptyArray), (JPath(".fr8y"),CNum)))))

    testAlign(sample.sortBy(_ \ "key"))
  }
}

// vim: set ts=4 sw=4 et:
