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

import blueeyes.json.{JPath,JPathField}
import blueeyes.json.JsonAST._

import org.specs2.mutable._

import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

trait TransformSpec extends TableModuleSpec {
  import trans._

  def checkTransformLeaf = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val table = fromJson(sample)
      val results = toJson(table.transform(Leaf(Source)))

      results must_== sample.data
    }
  }

  def testMap1IntLeaf = {
    val sample = (-10 to 10).map(JInt(_)).toStream
    val table = fromJson(SampleData(sample))
    val results = toJson(table.transform { Map1(Leaf(Source), lookupF1(Nil, "negate")) })

    results must_== (-10 to 10).map(x => JInt(-x))
  }

  /* Do we want to allow non-boolean sets to be used as filters without an explicit existence predicate?
  def checkTrivialFilter = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val table = fromJson(sample)
      val results = toJson(table.transform {
        Filter(
          Leaf(Source), 
          Leaf(Source)
        )
      })

      results must_== sample.data
    }
  }
  */

  def checkTrueFilter = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val table = fromJson(sample)
      val results = toJson(table.transform {
        Filter(
          Leaf(Source), 
          Map1(Leaf(Source), lookupF1(Nil, "true"))
        )
      })

      results must_== sample.data
    }
  }

  def checkFilter = {
    implicit val gen = sample(_ => Gen.value(Seq(JPath.Identity -> CLong)))
    check { (sample: SampleData) =>
      val table = fromJson(sample)
      val results = toJson(table.transform {
        Filter(
          Leaf(Source), 
          Map1(
            DerefObjectStatic(Leaf(Source), JPathField("value")), 
            lookupF2(Nil, "mod").applyr(CLong(2)) andThen lookupF2(Nil, "eq").applyr(CLong(0))
          )
        )
      })

      val expected = sample.data map { jv =>
        (jv \ "value") match { 
          case JInt(x) if x.longValue % 2 == 0 => jv
          case _ => JNothing 
        }
      }

      results must_== expected
    }
  }

  def checkObjectDeref = {
    implicit val gen = sample(objectSchema(_, 3))
    check { (sample: SampleData) =>
      val (field, _) = sample.schema.get.head
      val fieldHead = field.head.get
      val table = fromJson(sample)
      val results = toJson(table.transform {
        DerefObjectStatic(Leaf(Source), fieldHead.asInstanceOf[JPathField])
      })

      val expected = sample.data.map { jv => jv(JPath(fieldHead)) }

      results must_== expected
    }
  }

  def checkMap2 = {
    implicit val gen = sample(_ => Seq(JPath("value1") -> CLong, JPath("value2") -> CLong))
    check { (sample: SampleData) =>
      val table = fromJson(sample)
      val results = toJson(table.transform {
        Map2(
          DerefObjectStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathField("value1")),
          DerefObjectStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathField("value2")),
          lookupF2(Nil, "add")
        )
      })

      val expected = sample.data map { jv =>
        ((jv \ "value" \ "value1"), (jv \ "value" \ "value2")) match {
          case (JInt(x), JInt(y)) => JInt(x+y)
          case _ => failure("Bogus test data")
        }
      }

      results must_== expected
    }
  }

  def checkEqualSelf = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val table = fromJson(sample)
      val results = toJson(table.transform {
        Equal(Leaf(Source), Leaf(Source))
      })

      results must_== (Stream.tabulate(sample.data.size) { _ => JBool(true) })
    }
  }

  def checkEqual = {
    val genBase: Gen[SampleData] = sample(_ => Seq(JPath("value1") -> CLong, JPath("value2") -> CLong)).arbitrary
    implicit val gen: Arbitrary[SampleData] = Arbitrary {
      genBase map { sd =>
        SampleData(
          sd.data.zipWithIndex map {
            case (jv, i) if i%2 == 0 => 
              // construct object with value1 == value2
              jv.set(JPath("value/value2"), jv(JPath("value/value1")))

            case (jv, i) if i%5 == 0 => // delete value1
              jv.set(JPath("value/value1"), JNothing)

            case (jv, i) if i%5 == 3 => // delete value2
              jv.set(JPath("value/value2"), JNothing)

            case (jv, _) => jv
          }
        )
      }
    }

    check { (sample: SampleData) =>
      val table = fromJson(sample)
      val results = toJson(table.transform {
        Equal(
          DerefObjectStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathField("value1")),
          DerefObjectStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathField("value2"))
        )
      })

      val expected = sample.data.map { jv =>
        ((jv \ "value" \ "value1"), (jv \ "value" \ "value2")) match {
          case (JInt(x), JInt(y))  => JBool(x == y)
          case (JNothing, JInt(y)) => JNothing
          case (JInt(x), JNothing) => JNothing
          case _ => failure("Bogus test data")
        }
      }

      results must_== expected
    }
  }
}

// vim: set ts=4 sw=4 et:
