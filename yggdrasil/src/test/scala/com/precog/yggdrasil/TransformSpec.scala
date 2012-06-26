package com.precog.yggdrasil

import blueeyes.json.JPath

import org.specs2.mutable._

import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

trait TransformSpec extends TableModuleSpec {
  import transforms._

  def checkTransformLeaf = check { (sample: SampleData) =>
    val table = fromJson(sample)
    val results = toJson(table.transform(Map(JPath.Identity -> Leaf(Source))))

    sample must_== results
  }

  def checkFilter = check { (sample: SampleData) =>
    val table = fromJson(sample)
    val results = toJson(table.transform {
      Map(
        JPath.Identity -> Filter(
          Leaf(Source), 
          Map1(Leaf(Source)) {
            liftF1({ case _ => CBoolean(true) }) 
          }
        ))
    })

    sample must_== results
  }
}

// vim: set ts=4 sw=4 et:
