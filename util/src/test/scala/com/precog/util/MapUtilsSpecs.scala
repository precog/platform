package com.precog.util

import org.specs2.ScalaCheck
import org.specs2.mutable._
import org.scalacheck._

import scalaz.Either3

object MapUtilsSpecs extends Specification with ScalaCheck with MapUtils {
  import Prop._
  
  "cogroup" should {
    "produce left, right and middle cases" in check { (left: Map[Int, List[Int]], right: Map[Int, List[Int]]) =>
      val result = left cogroup right
      
      val leftKeys = left.keySet -- right.keySet
      val rightKeys = right.keySet -- left.keySet
      val middleKeys = left.keySet & right.keySet
      
      val leftContrib = leftKeys.toSeq flatMap { key =>
        left(key) map { key -> Either3.left3[Int, (List[Int], List[Int]), Int](_) }
      }
      
      val rightContrib = rightKeys.toSeq flatMap { key =>
        right(key) map { key -> Either3.right3[Int, (List[Int], List[Int]), Int](_) }
      }
      
      val middleContrib = middleKeys.toSeq map { key =>
        key -> Either3.middle3[Int, (List[Int], List[Int]), Int]((left(key), right(key)))
      }
      
      result must containAllOf(leftContrib)
      result must containAllOf(rightContrib)
      result must containAllOf(middleContrib)
      result must haveSize(leftContrib.length + rightContrib.length + middleContrib.length)
    }
  }
}
