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

  val idSource = new IdSource {
    private val source = new java.util.concurrent.atomic.AtomicLong
    def nextId() = source.getAndIncrement
  }

  type Record[A] = (Identities, A)

  case class IdCount(idCount: Int)

  implicit def genLong = for (l <- arbitrary[Long]) yield (l: Long)

  implicit def recGen[A](implicit idCount: IdCount, agen: Gen[A]): Gen[Record[A]] = 
    for {
      ids <- listOfN(idCount.idCount, arbitrary[Long])
      value <- agen
    } yield {
      (VectorCase(ids: _*), value)
    }

  implicit def dsGen[A](implicit idCount: IdCount, rgen: Gen[Record[A]]): Gen[IterableDataset[A]] = {
    for (l <- listOf(rgen)) yield IterableDataset(idCount.idCount, l)
  }

  implicit def arbIterableDataset[A](implicit idCount: IdCount, gen: Gen[Record[A]]): Arbitrary[IterableDataset[A]] =
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
      implicit val idCount = IdCount(1)
      check { (l1: IterableDataset[Long], l2: IterableDataset[Long]) => 
        val results = l1.crossLeft(l2) { 
          case (a, b) => (a, b)
        }

        results.iterator.toList must_== l1.iterator.toList.flatMap(i => l2.iterator.toList.map((j: Long) => (i, j)))
      } 
    }

    "crossRight" in {
      implicit val idCount = IdCount(1)
      check { (l1: IterableDataset[Long], l2: IterableDataset[Long]) => 
        val results = l1.crossRight(l2) { 
          case (a, b) => (a, b)
        }

        results.iterator.toList must_== l2.iterator.toList.flatMap(i => l1.iterator.toList.map((j: Long) => (j, i)))
      } 
    }

    "paddedMerge" in {
      case class MergeSample(maxIds: Int, l1: IterableDataset[Long], l2: IterableDataset[Long])
      implicit val mergeSampleArbitrary = Arbitrary(
        for {
          i1 <- choose(0, 3) map (IdCount(_: Int))
          i2 <- choose(1, 3) map (IdCount(_: Int)) if i2 != i1
          l1 <- dsGen(i1, recGen(i1, genLong))
          l2 <- dsGen(i2, recGen(i2, genLong))
        } yield {
          MergeSample(i1.idCount max i2.idCount, l1, l2)
        }
      )

      check { (sample: MergeSample) => 
        val results = sample.l1.paddedMerge(sample.l2, () => idSource.nextId()).iterator.toList
        val sampleLeft = sample.l1.iterator.toList
        val sampleRight = sample.l2.iterator.toList

        (results must haveSize(sampleLeft.size + sampleRight.size)) and
        (results must haveTheSameElementsAs(sampleLeft ++ sampleRight))
      } 
    }
  }
}
