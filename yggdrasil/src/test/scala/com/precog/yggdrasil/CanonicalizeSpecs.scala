package com.precog.yggdrasil
package table

import blueeyes.json._

import scalaz.{Copointed, StreamT}
import scalaz.syntax.copointed._

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck.Gen

trait CanonicalizeSpec[M[+_]] extends ColumnarTableModuleTestSupport[M] with Specification with ScalaCheck {
  import SampleData._

  val table = {
    val JArray(elements) = JParser.parse("""[
      {"foo":1},
      {"foo":2},
      {"foo":3},
      {"foo":4},
      {"foo":5},
      {"foo":6},
      {"foo":7},
      {"foo":8},
      {"foo":9},
      {"foo":10},
      {"foo":11},
      {"foo":12},
      {"foo":13},
      {"foo":14}
    ]""")
    
    val sample = SampleData(elements.toStream)
    fromSample(sample)
  }

  def checkBoundedCanonicalize = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val size = sample.data.size
      val minLength = Gen.choose(0, size / 2).sample.get
      val maxLength = minLength + Gen.choose(1, size / 2 + 1).sample.get

      val canonicalizedTable = table.canonicalize(minLength, Some(maxLength))
      val slices = canonicalizedTable.slices.toStream.copoint map (_.size)
      if (size > 0) {
        slices.init must haveAllElementsLike { case (sliceSize: Int) =>
          sliceSize must beBetween(minLength, maxLength)
        }
        slices.last must be_<=(maxLength)
      } else {
        slices must haveSize(0)
      }
    }
  }

  def checkCanonicalize = {
    implicit val gen = sample(schema)  
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val size = sample.data.size
      val length = Gen.choose(1, size + 3).sample.get

      val canonicalizedTable = table.canonicalize(length)
      val resultSlices = canonicalizedTable.slices.toStream.copoint
      val resultSizes = resultSlices.map(_.size)

      val expected = {
        val num = size / length
        val remainder = size % length
        val prefix = Stream.fill(num)(length)
        if (remainder > 0) prefix :+ remainder else prefix
      }

      resultSizes mustEqual expected
    }
  }.set(minTestsOk -> 1000)

  def testCanonicalize = {
    val result = table.canonicalize(3)

    val slices = result.slices.toStream.copoint
    val sizes = slices.map(_.size)

    sizes mustEqual Stream(3, 3, 3, 3, 2)
  }

  def testCanonicalizeZero = {
    table.canonicalize(0) must throwA[IllegalArgumentException]
  }

  def testCanonicalizeBoundary = {
    val result = table.canonicalize(5)

    val slices = result.slices.toStream.copoint
    val sizes = slices.map(_.size)

    sizes mustEqual Stream(5, 5, 4)
  }

  def testCanonicalizeOverBoundary = {
    val result = table.canonicalize(12)

    val slices = result.slices.toStream.copoint
    val sizes = slices.map(_.size)

    sizes mustEqual Stream(12, 2)
  }

  def testCanonicalizeEmptySlices = {
    def tableTakeRange(table: Table, start: Int, numToTake: Long) = 
      table.takeRange(start, numToTake).slices.toStream.copoint

    val emptySlice = Slice(Map(), 0)
    val slices = 
      Stream(emptySlice) ++ tableTakeRange(table, 0, 5) ++
      Stream(emptySlice) ++ tableTakeRange(table, 5, 4) ++
      Stream(emptySlice) ++ tableTakeRange(table, 9, 5) ++
      Stream(emptySlice)

    val toPrint = slices.map(_.size)

    val newTable = Table(StreamT.fromStream(M.point(slices)), table.size)
    val result = newTable.canonicalize(4)

    val resultSlices = result.slices.toStream.copoint
    val resultSizes = resultSlices.map(_.size)

    resultSizes mustEqual Stream(4, 4, 4, 2)
  }

  def testCanonicalizeEmpty = {
    val table = Table.empty
    
    val result = table.canonicalize(3)

    val slices = result.slices.toStream.copoint
    val sizes = slices.map(_.size)

    sizes mustEqual Stream()
  }
}
