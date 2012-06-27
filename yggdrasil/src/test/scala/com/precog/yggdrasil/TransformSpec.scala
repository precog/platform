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
