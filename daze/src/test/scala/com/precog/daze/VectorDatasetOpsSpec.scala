package com.precog
package daze

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.anyVal._
import Iteratee._
import Either3._

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck.Gen._

class VectorDatasetOpsSpec extends Specification with ScalaCheck {
  val ops = new VectorDatasetOps { }
  "iterable dataset ops" should {
    "cogroup" in {
      val v1  = Vector(1, 3, 3, 5, 7, 8, 8)
      val v2  = Vector(2, 3, 4, 5, 5, 6, 8, 8)

      implicit val order: (Int, Int) => Ordering = Order[Int].order _
      
      val expected = Vector(
        left3(1),
        right3(2),
        middle3((3, 3)),
        middle3((3, 3)),
        right3(4),
        middle3((5, 5)),
        middle3((5, 5)),
        right3(6),
        left3(7),
        middle3((8, 8)),
        middle3((8, 8)),
        middle3((8, 8)),
        middle3((8, 8)) 
      )

      val results = ops.cogroup(v1, v2) {
        new CogroupF[Int, Int, Either3[Int, (Int, Int), Int]](false) {
          def left(i: Int) = left3(i)
          def both(i1: Int, i2: Int) = middle3((i1, i2))
          def right(i: Int) = right3(i)
        }
      }

      Vector(results.toSeq: _*) must_== expected
    }

    "join" in {
      val v1  = Vector(1, 3, 3, 5, 7, 8, 8)
      val v2  = Vector(2, 3, 4, 5, 5, 6, 8, 8)

      implicit val order: (Int, Int) => Ordering = Order[Int].order _
      
      val expected = Vector(
        middle3((3, 3)),
        middle3((3, 3)),
        middle3((5, 5)),
        middle3((5, 5)),
        middle3((8, 8)),
        middle3((8, 8)),
        middle3((8, 8)),
        middle3((8, 8)) 
      )

      val results = ops.cogroup(v1, v2) {
        new CogroupF[Int, Int, Either3[Int, (Int, Int), Int]](true) {
          def left(i: Int) = left3(i)
          def both(i1: Int, i2: Int) = middle3((i1, i2))
          def right(i: Int) = right3(i)
        }
      }

      Vector(results.toSeq: _*) must_== expected
    }

    "crossLeft" in {
      check { (l1: List[Int], l2: List[Int]) => 
        val results = ops.crossLeft(l1, l2) { Tuple2.apply _ }
        results.toList must_== l1.flatMap(i => l2.map((i, _)))
      } 
    }

    "crossRight" in {
      check { (l1: List[Int], l2: List[Int]) => 
        val results = ops.crossRight(l1, l2) { Tuple2.apply _ }
        results.toList must_== l2.flatMap(i => l1.map((_, i)))
      } 
    }
  }
}
