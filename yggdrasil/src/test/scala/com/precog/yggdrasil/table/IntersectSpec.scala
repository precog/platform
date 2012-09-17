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
    
