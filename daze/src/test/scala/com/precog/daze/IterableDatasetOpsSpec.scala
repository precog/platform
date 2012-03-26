/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog
package daze

import memoization._
import yggdrasil._
import yggdrasil.serialization._
import com.precog.common._
import com.precog.common.util.IOUtils

import blueeyes.util.Clock
import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext

import java.io.{DataInputStream, DataOutputStream, File}

import scalaz.{NonEmptyList => NEL, _}
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

trait IterableDatasetGenerators {
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
    for (l <- listOf(rgen)) yield IterableDataset(idCount.idCount, l.distinct)
  }

  implicit def arbIterableDataset[A](implicit idCount: IdCount, gen: Gen[Record[A]]): Arbitrary[IterableDataset[A]] =
     Arbitrary(dsGen[A])
}

class IterableDatasetOpsSpec extends Specification with ScalaCheck with IterableDatasetOpsComponent with IterableDatasetGenerators 
with DiskIterableDatasetMemoizationComponent {
  type YggConfig = IterableDatasetOpsConfig with DiskMemoizationConfig
  override type Dataset[α] = IterableDataset[α]

  object ops extends Ops
  import ops._

  object yggConfig extends IterableDatasetOpsConfig with DiskMemoizationConfig {
    def sortBufferSize: Int = 1000
    def sortWorkDir: File = IOUtils.createTmpDir("idsoSpec")
    def clock = Clock.System
    implicit object memoSerialization extends IncrementalSerialization[(Identities, Long)] with TestRunlengthFormatting with ZippedStreamSerialization
    def memoizationBufferSize: Int = 1000
    def memoizationWorkDir: File = IOUtils.createTmpDir("idsoSpecMemo")
  }

  def rec(i: Long) = (VectorCase(i), i: Long)
  def unstableRec(i: Long) = (VectorCase(idSource.nextId()), i: Long)

  val actorSystem = ActorSystem("stub_operations_api")
  implicit val asyncContext: akka.dispatch.ExecutionContext = ExecutionContext.defaultExecutionContext(actorSystem)

  var memoCtx: MemoContext = _
  withMemoizationContext((ctx: MemoContext) => memoCtx = ctx)

  val idSource = new IdSource {
    private val source = new java.util.concurrent.atomic.AtomicLong
    def nextId() = source.getAndIncrement
  }

  def expiration = System.currentTimeMillis + 10000

  trait TestRunlengthFormatting extends RunlengthFormatting[(Identities, Long)] {
    type Header = Int
    
    def headerFor(value: (Identities, Long)) = value._1.length
    
    def writeHeader(out: DataOutputStream, header: Int) {
      out.writeInt(header)
    }
    
    def readHeader(in: DataInputStream) = in.readInt()
    
    def writeRecord(out: DataOutputStream, value: (Identities, Long)) {
      val (ids, v) = value
      
      ids foreach out.writeLong
      out.writeLong(v)
    }
    
    def readRecord(in: DataInputStream, header: Int) = {
      val ids = VectorCase((0 until header) map { _ => in.readLong() }: _*)
      val v = in.readLong()
      
      (ids, v)
    }
  }

  import yggConfig.memoSerialization

  implicit object GroupingSortSerialization extends SortSerialization[(Long, Identities, Long)] with RunlengthFormatting[(Long, Identities, Long)] with ZippedStreamSerialization {
    type Header = Int
    
    def headerFor(value: (Long, Identities, Long)) = value._2.length
    
    def writeHeader(out: DataOutputStream, header: Int) {
      out.writeInt(header)
    }
    
    def readHeader(in: DataInputStream) = in.readInt()
    
    def writeRecord(out: DataOutputStream, value: (Long, Identities, Long)) {
      val (k, ids, v) = value
      
      out.writeLong(k)
      ids foreach out.writeLong
      out.writeLong(v)
    }
    
    def readRecord(in: DataInputStream, header: Int) = {
      val k = in.readLong()
      val ids = VectorCase((0 until header) map { _ => in.readLong() }: _*)
      val v = in.readLong()
      
      (k, ids, v)
    }
  }
  
  implicit object GroupingEventSortSerialization extends SortSerialization[(Identities, Long)] with TestRunlengthFormatting with ZippedStreamSerialization
  
  
  "iterable dataset ops" should {
    "cogroup" in {
      // TODO: Enhance test coverage
      "abitrary datasets" in {
        true mustEqual false
      }.pendingUntilFixed(" - TODO: ")

      "static full dataset" in {
        val v1  = IterableDataset(1, Vector(rec(0), rec(1), rec(3), rec(3), rec(5), rec(7), rec(8), rec(8)))
        val v2  = IterableDataset(1, Vector(rec(0), rec(2), rec(3), rec(4), rec(5), rec(5), rec(6), rec(8), rec(8)))

        val expected = Vector(
          middle3((0, 0)),
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

        Vector(results.iterator.toSeq: _*) mustEqual expected
      }

      "static single pair dataset" in {
        // Catch bug where equal at the end of input produces middle, right
        val v1s = IterableDataset(1, Vector(rec(0)))
        val v2s = IterableDataset(1, Vector(rec(0)))

        val expected2 = Vector(middle3((0, 0)))

        val results2 = v1s.cogroup(v2s) {
          new CogroupF[Long, Long, Either3[Long, (Long, Long), Long]] {
            def left(i: Long) = left3(i)
            def both(i1: Long, i2: Long) = middle3((i1, i2))
            def right(i: Long) = right3(i)
          }
        }

        Vector(results2.iterator.toSeq: _*) mustEqual expected2
      }
    }

    "join" in {
      // TODO: Enhance test coverage
      "arbitrary dataset" in { true mustEqual false }.pendingUntilFixed(" - TODO: ")

      "static full dataset" in {
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
    
    "union" in {
      implicit val ordering = identityValueOrder[Long].toScalaOrdering
      implicit val idCount = IdCount(1)
      check { (l1: IterableDataset[Long], l2: IterableDataset[Long]) => 
        val results = l1.union(l2).iterable.toList
        val expectedSet = Set((l1.iterable ++ l2.iterable).toSeq: _*).toList
        val expectedSorted = expectedSet.sorted

        results must containAllOf(expectedSorted).only.inOrder
      }
    }

    "intersect" in {
      implicit val ordering = identityValueOrder[Long].toScalaOrdering
      implicit val idCount = IdCount(1)
      check { (l1: IterableDataset[Long], l2: IterableDataset[Long]) => 
        val results = l1.intersect(l2).iterable.toList
        val expectedSet = (Set(l1.iterable.toSeq: _*) & Set(l2.iterable.toSeq: _*)).toList
        val expectedSorted = expectedSet.sorted

        results must containAllOf(expectedSorted).only.inOrder
      }
    }

    "group according to an integer key" in {
      val groups = Stream(
        0L -> Vector(rec(1)),
        2L -> Vector(rec(2), rec(3), rec(3)),
        4L -> Vector(rec(5)),
        6L -> Vector(rec(6), rec(7)),
        8L -> Vector(rec(8)))
      
      val ds = IterableDataset(1, groups.unzip._2 reduce { _ ++ _ })
      
      val result = ds.group(0, memoCtx, expiration) { num =>
        IterableDataset(1, Vector(rec((num / 2) * 2)))
      }

      val result2 = mapGrouping(result) { ds =>
        val IterableDataset(count, str) = ds
        IterableDataset(count, Vector(str.toSeq: _*))
      }
      
      val expected = groups map {
        case (k, v) => (k, IterableDataset(1, v))
      }
      
      result2.iterator.toList must_== expected.toList
    }
    
    "group according to an identity key" in {
      val groups = Stream(
        1L -> Vector(rec(1)),
        2L -> Vector(rec(2)),
        3L -> Vector(rec(3)),
        5L -> Vector(rec(5)),
        6L -> Vector(rec(6)),
        7L -> Vector(rec(7)),
        8L -> Vector(rec(8)))
      
      val ds = IterableDataset(1, groups.unzip._2 reduce { _ ++ _ })
      
      val result = ds.group(0, memoCtx, expiration) { num =>
        IterableDataset(1, Vector(rec(num)))
      }

      val result2 = mapGrouping(result) { ds =>
        val IterableDataset(count, str) = ds
        IterableDataset(count, Vector(str.toSeq: _*))
      }
      
      val expected = groups map {
        case (k, v) => (k, IterableDataset(1, v))
      }
      
      result2.iterator.toList must_== expected.toList
    }
  }
  
  "iterable grouping ops" should {
    
    val sharedRecs = Map(
      'i1 -> unstableRec(1),
      'i2 -> unstableRec(2),
      'i3a -> unstableRec(3),
      'i3b -> unstableRec(3),
      'i4 -> unstableRec(4),
      'i5 -> unstableRec(5),
      'i6 -> unstableRec(6),
      'i7 -> unstableRec(7),
      'i8a -> unstableRec(8),
      'i8b -> unstableRec(8))
    
    def g1 = {
      val seed = Stream(
        0L -> Vector(sharedRecs('i1)),
        2L -> Vector(sharedRecs('i2), sharedRecs('i3a), sharedRecs('i3b)),
        4L -> Vector(sharedRecs('i5)),
        6L -> Vector(sharedRecs('i6), sharedRecs('i7)),
        8L -> Vector(sharedRecs('i8a)))
        
      val itr = seed map {
        case (k, v) => (k, IterableDataset(1, v))
      } iterator
      
      IterableGrouping(itr)
    }
    
    def g2 = {
      val seed = Stream(
        1L -> Vector(sharedRecs('i1)),
        2L -> Vector(sharedRecs('i2), sharedRecs('i3a), sharedRecs('i4)),
        5L -> Vector(sharedRecs('i5), sharedRecs('i6)),
        7L -> Vector(sharedRecs('i7)),
        8L -> Vector(sharedRecs('i8b)))
        
      val itr = seed map {
        case (k, v) => (k, IterableDataset(1, v))
      } iterator
      
      IterableGrouping(itr)
    }
    
    "implement mapping" in {
      g1.iterator.size must_== 5
      val result = mapGrouping(g1) { v =>
        v.iterable.size
      }
      
      result.iterator.toList must containAllOf(List(
        0L -> 1,
        2L -> 3,
        4L -> 1,
        6L -> 2,
        8L -> 1)).only.inOrder
    }
    
    "implement merging" >> {
      "union" >> {
        val result = mergeGroups(g1, g2, true).iterator.map {
          case (k, v) => (k, IterableDataset(v.idCount, Vector(v.iterable.toSeq: _*)))
        }
        
        val expected = Stream(
          0L -> IterableDataset(1, Vector(sharedRecs('i1))),
          1L -> IterableDataset(1, Vector(sharedRecs('i1))),
          2L -> IterableDataset(1, Vector(sharedRecs('i2), sharedRecs('i3a), sharedRecs('i3b), sharedRecs('i4))),
          4L -> IterableDataset(1, Vector(sharedRecs('i5))),
          5L -> IterableDataset(1, Vector(sharedRecs('i5), sharedRecs('i6))),
          6L -> IterableDataset(1, Vector(sharedRecs('i6), sharedRecs('i7))),
          7L -> IterableDataset(1, Vector(sharedRecs('i7))),
          8L -> IterableDataset(1, Vector(sharedRecs('i8a), sharedRecs('i8b))))
          
        result.toList mustEqual expected.toList
      }
      
      "intersect" >> {
        val result = mergeGroups(g1, g2, false).iterator.map {
          case (k, v) => (k, IterableDataset(v.idCount, Vector(v.iterable.toSeq: _*)))
        }

        val expected = List(
          2L -> IterableDataset(1, Vector(sharedRecs('i2), sharedRecs('i3a))),
          8L -> IterableDataset(1, Vector()))
          
        result.toList mustEqual expected.toList
      }
    }
    
    "implement zipping" in {
      val mappedG1 = mapGrouping(g1) { v => NEL(v) }
      val mappedG2 = mapGrouping(g2) { v => NEL(v) }

      val result = zipGroups(mappedG1, mappedG2)
      
      val expected = Stream(
        2L -> NEL(IterableDataset(1, Vector(sharedRecs('i2), sharedRecs('i3a), sharedRecs('i3b))),
                  IterableDataset(1, Vector(sharedRecs('i2), sharedRecs('i3a), sharedRecs('i4)))),
        8L -> NEL(IterableDataset(1, Vector(sharedRecs('i8a))),
                  IterableDataset(1, Vector(sharedRecs('i8b)))))
          
      result.iterator.toList must containAllOf(expected).only.inOrder
    }

  }
}
