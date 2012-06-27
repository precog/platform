package com.precog.yggdrasil

import blueeyes.json.JPath
import blueeyes.json.JsonAST._

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

    results must_== sample.data
  }

  def testMap1IntLeaf = {
    val sample = (-10 to 10).map(JInt(_)).toStream
    val table = fromJson(SampleData(sample))
    val results = toJson(table.transform { Map1(Leaf(Source), lookupF1(Vector(), "negate")) })

    results must_== (-10 to 10).map(x => JInt(-x))
  }

  def checkTrueFilter = check { (sample: SampleData) =>
    val table = fromJson(sample)
    val results = toJson(table.transform {
      Filter(
        Leaf(Source), 
        Map1(Leaf(Source), lookupF1(Vector(), "true"))
      )
    })

    results must_== sample.data
  }
}

// vim: set ts=4 sw=4 et:
