package com.precog
package daze

import yggdrasil._
import com.precog.common._

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.anyVal._
import Iteratee._
import Either3._

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

class IterableDatasetOpsSpec extends Specification with ScalaCheck with IterableDatasetOpsComponent {
  object ops extends Ops
  import ops._

  "iterable dataset ops" should {
    /*
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
    */

    "join" in {
      def rec(i: Long) = (VectorCase(i), i: java.lang.Long)
      val v1  = IterableDataset(1, Vector(rec(1), rec(3), rec(3), rec(5), rec(7), rec(8), rec(8)))
      val v2  = IterableDataset(1, Vector(rec(2), rec(3), rec(4), rec(5), rec(5), rec(6), rec(8), rec(8)))
      
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

      val results = v1.join(v2, 1) {
        case (a, b) => (a, b)
      }

      Vector(results.toSeq: _*) must_== expected
    }

    type Record[A <: AnyVal] = (Identities, A)

    implicit def genLong = for (l <- arbitrary[Long]) yield (l: java.lang.Long)

    implicit def recGen[A <: AnyRef](implicit agen: Gen[A]): Gen[Record[A]] = 
      for {
        id <- arbitrary[Long]
        value <- agen
      } yield {
        (VectorCase(id), value)
      }

    implicit def dsGen[A <: AnyRef](implicit rgen: Gen[Record[A]]): Gen[IterableDataset[A]] = {
      for (l <- listOf(rgen)) yield IterableDataset(1, l)
    }

    implicit def arbIterableDataset[A <: AnyRef](implicit gen: Gen[Record[A]]): Arbitrary[IterableDataset[A]] =
       Arbitrary(dsGen[A])

    "crossLeft" in {
      check { (l1: IterableDataset[java.lang.Long], l2: IterableDataset[java.lang.Long]) => 
        val results = l1.crossLeft(l2) { 
          case (a, b) => (a, b)
        }

        results.toList must_== l1.flatMap(i => l2.map((j: java.lang.Long) => (i, j)))
      } 
    }

    "crossRight" in {
      check { (l1: IterableDataset[java.lang.Long], l2: IterableDataset[java.lang.Long]) => 
        val results = l1.crossRight(l2) { 
          case (a, b) => (a, b)
        }

        results.toList must_== l2.flatMap(i => l1.map((j: java.lang.Long) => (j, i)))
      } 
    }
  }
}
