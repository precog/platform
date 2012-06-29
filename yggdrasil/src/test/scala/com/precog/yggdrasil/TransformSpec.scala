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
}

// vim: set ts=4 sw=4 et:
