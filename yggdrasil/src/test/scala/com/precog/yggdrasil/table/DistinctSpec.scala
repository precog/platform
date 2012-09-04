package com.precog.yggdrasil
package table

import scala.collection.immutable.BitSet
import scala.util.Random

import blueeyes.json._
import blueeyes.json.JsonAST._

import scalaz.StreamT
import scalaz.syntax.copointed._

import org.specs2.ScalaCheck
import org.specs2.mutable._

trait DistinctSpec[M[+_]] extends TestColumnarTableModule[M] with TableModuleTestSupport[M] with Specification with ScalaCheck {
  import SampleData._
  import trans._

  def testDistinctIdentity = {
    implicit val gen = sort(distinct(sample(schema)))
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      
      val distinctTable = table.distinct(Leaf(Source))
      
      val result = toJson(distinctTable)

      result.copoint must_== sample.data
    }
  }

  def testDistinct = {
    implicit val gen = sort(duplicateRows(sample(schema)))
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      
      val distinctTable = table.distinct(Leaf(Source))
      
      val result = toJson(distinctTable).copoint
      
      result must_== result.distinct
    }.set(minTestsOk -> 500)
  }
}
