package com.precog
package daze

import yggdrasil._
import yggdrasil.serialization._
import com.precog.common._

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
   
  
  implicit object GroupingSortSerialization extends BaseSortSerialization[(Long, Identities, Long)] with ZippedStreamSerialization {
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
  
  implicit object GroupingEventSortSerialization extends BaseSortSerialization[(Identities, Long)] with ZippedStreamSerialization {
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
    
    "group according to an integer key" in {
      val groups = Stream(
        0L -> Vector(rec(1)),
        2L -> Vector(rec(2), rec(3), rec(3)),
        4L -> Vector(rec(5)),
        6L -> Vector(rec(6), rec(7)),
        8L -> Vector(rec(8)))
      
      val ds = IterableDataset(1, groups.unzip._2 reduce { _ ++ _ })
      
      val result = ds.group(0) { num =>
        IterableDataset(1, Vector(rec((num / 2) * 2)))
      }
      
      val expected = groups map {
        case (k, v) => (k, IterableDataset(1, v))
      }
      
      result.iterator.toStream mustEqual expected
    }
  }
  
  "iterable grouping ops" should {
    
    val sharedRecs = Map(
      'i1 -> rec(1),
      'i2 -> rec(2),
      'i3a -> rec(3),
      'i3b -> rec(3),
      'i4 -> rec(4),
      'i5 -> rec(5),
      'i6 -> rec(6),
      'i7 -> rec(7),
      'i8a -> rec(8),
      'i8b -> rec(8))
    
    val g1 = {
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
    
    val g2 = {
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
      val result = mapGrouping(g1) { v =>
        v.iterable.size
      }
      
      result.iterator.toStream mustEqual Stream(
        0L -> 1,
        2L -> 3,
        4L -> 1,
        6L -> 2,
        8L -> 1)
    }
    
    "implement merging" >> {
      "union" >> {
        val result = mergeGroups(g1, g2, true)
        
        val expected = Stream(
          0L -> Vector(sharedRecs('i1)),
          1L -> Vector(sharedRecs('i1)),
          2L -> Vector(sharedRecs('i2), sharedRecs('i3a), sharedRecs('i3b), sharedRecs('i4)),
          4L -> Vector(sharedRecs('i5)),
          5L -> Vector(sharedRecs('i5), sharedRecs('i6)),
          6L -> Vector(sharedRecs('i6), sharedRecs('i7)),
          7L -> Vector(sharedRecs('i7)),
          8L -> Vector(sharedRecs('i8a), sharedRecs('i8b)))
          
        result.iterator.toStream mustEqual expected
      }
      
      "intersect" >> {
        val result = mergeGroups(g1, g2, false)
        
        val expected = Stream(
          2L -> Vector(sharedRecs('i2)),
          6L -> Vector(sharedRecs('i6), sharedRecs('i7)),
          8L -> Vector())
          
        result.iterator.toStream mustEqual expected
      }
    }
    
    "implement zipping" in {
      val mappedG1 = mapGrouping(g1) { v => NEL(v) }
      val mappedG2 = mapGrouping(g2) { v => NEL(v) }
      
      val result = zipGroups(mappedG1, mappedG2)
      
      val expected = Stream(
        0L -> NEL(IterableDataset(1, Vector(sharedRecs('i1)))),
        1L -> NEL(IterableDataset(1, Vector(sharedRecs('i1)))),
        2L -> NEL(IterableDataset(1, Vector(sharedRecs('i2), sharedRecs('i3a), sharedRecs('i3b))),
          IterableDataset(1, Vector(sharedRecs('i2), sharedRecs('i3a), sharedRecs('i4)))),
        4L -> NEL(IterableDataset(1, Vector(sharedRecs('i5)))),
        5L -> NEL(IterableDataset(1, Vector(sharedRecs('i5), sharedRecs('i6))),
          IterableDataset(1, Vector(sharedRecs('i5), sharedRecs('i6)))),
        6L -> NEL(IterableDataset(1, Vector(sharedRecs('i6), sharedRecs('i7)))),
        7L -> NEL(IterableDataset(1, Vector(sharedRecs('i7)))),
        8L -> NEL(IterableDataset(1, Vector(sharedRecs('i8a))),
          IterableDataset(1, Vector(sharedRecs('i8b)))))
          
      result.iterator.toStream mustEqual expected
    }

    "union" in {
      implicit val ordering = tupledIdentitiesOrder[Long].toScalaOrdering
      implicit val idCount = IdCount(1)
      check { (l1: IterableDataset[Long], l2: IterableDataset[Long]) => 
        val results = l1.union(l2).iterable.iterator.toList

        results must_== Set(l1.iterable ++ l2.iterable).toList.sorted
      }
    }
  }
}
