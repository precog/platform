package com.precog
package daze

import yggdrasil._
import com.precog.common._

import java.io.File
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
  type YggConfig = SortConfig

  object ops extends Ops
  import ops._

  object yggConfig extends SortConfig {
    def sortBufferSize: Int = 1000
    def sortWorkDir: File = new File("/tmp")
  }

  def rec(i: Long) = (VectorCase(i), i: Long)

  type Record[A] = (Identities, A)

  implicit def genLong = for (l <- arbitrary[Long]) yield (l: Long)

  implicit def recGen[A](implicit agen: Gen[A]): Gen[Record[A]] = 
    for {
      id <- arbitrary[Long]
      value <- agen
    } yield {
      (VectorCase(id), value)
    }

  implicit def dsGen[A](implicit rgen: Gen[Record[A]]): Gen[IterableDataset[A]] = {
    for (l <- listOf(rgen)) yield IterableDataset(1, l)
  }

  implicit def arbIterableDataset[A](implicit gen: Gen[Record[A]]): Arbitrary[IterableDataset[A]] =
     Arbitrary(dsGen[A])

  "iterable dataset ops" should {
    "cogroup" in {
      val v1  = IterableDataset(1, Vector(rec(1), rec(3), rec(3), rec(5), rec(7), rec(8), rec(8)))
      val v2  = IterableDataset(1, Vector(rec(2), rec(3), rec(4), rec(5), rec(5), rec(6), rec(8), rec(8)))

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

      val results = v1.cogroup(v2) {
        new CogroupF[Long, Long, Either3[Long, (Long, Long), Long]] {
          def left(i: Long) = left3(i)
          def both(i1: Long, i2: Long) = middle3((i1, i2))
          def right(i: Long) = right3(i)
        }
      }

      Vector(results.iterator.toSeq: _*) must_== expected
    }

    "join" in {
      val v1  = IterableDataset(1, Vector(rec(1), rec(3), rec(3), rec(5), rec(7), rec(8), rec(8)))
      val v2  = IterableDataset(1, Vector(rec(2), rec(3), rec(4), rec(5), rec(5), rec(6), rec(8), rec(8)))
      
      val expected = Vector(
        (3, 3),
        (3, 3),
        (5, 5),
        (5, 5),
        (8, 8),
        (8, 8),
        (8, 8),
        (8, 8) 
      )

      val results = v1.join(v2, 1) {
        case (a, b) => (a, b)
      }

      Vector(results.iterator.toSeq: _*) must_== expected
    }

    "crossLeft" in {
      check { (l1: IterableDataset[Long], l2: IterableDataset[Long]) => 
        val results = l1.crossLeft(l2) { 
          case (a, b) => (a, b)
        }

        results.iterator.toList must_== l1.iterator.toList.flatMap(i => l2.iterator.toList.map((j: Long) => (i, j)))
      } 
    }

    "crossRight" in {
      check { (l1: IterableDataset[Long], l2: IterableDataset[Long]) => 
        val results = l1.crossRight(l2) { 
          case (a, b) => (a, b)
        }

        results.iterator.toList must_== l2.iterator.toList.flatMap(i => l1.iterator.toList.map((j: Long) => (j, i)))
      } 
    }
  }
}
