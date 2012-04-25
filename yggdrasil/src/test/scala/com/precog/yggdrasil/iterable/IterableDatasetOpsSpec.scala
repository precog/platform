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
package com.precog.yggdrasil
package iterable

import memoization._
import serialization._
import com.precog.common._
import com.precog.common.util.IOUtils
import com.precog.util.IdGen

import blueeyes.util.Clock
import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext

import java.io.{DataInputStream, DataOutputStream, File}

import scala.annotation.tailrec
import scalaz.Ordering._

import scalaz.{NonEmptyList => NEL, _}
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.anyVal._
import scalaz.std.tuple._
import Iteratee._
import Either3._

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._


class IterableDatasetOpsSpec extends Specification with ScalaCheck with IterableDatasetOpsComponent with IterableDatasetGenerators 
with DiskMemoizationComponent {
  type YggConfig = IterableDatasetOpsConfig with DiskMemoizationConfig
  override type Dataset[α] = IterableDataset[α]
  override type Memoable[α] = Iterable[α]

  override def defaultValues = super.defaultValues + (minTestsOk -> 1000)
  override val defaultPrettyParams = org.scalacheck.Pretty.Params(2)

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

  trait TestRunlengthFormatting extends RunlengthFormatting[(Identities, Long)] {
    type Header = Int
    
    def headerFor(value: (Identities, Long)) = value._1.length
    
    def writeHeader(out: DataOutputStream, header: Int) {
      out.writeInt(header)
    }
    
    def readHeader(in: DataInputStream) = in.readInt()
    
    def writeRecord(out: DataOutputStream, value: (Identities, Long), header: Header) {
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
    
    def writeRecord(out: DataOutputStream, value: (Long, Identities, Long), header: Header) {
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
  
  implicit object LongSortSerialization extends SortSerialization[Long] with ZippedStreamSerialization {
    type Header = Unit
    
    def headerFor(value: Long) {}
    def writeHeader(out: DataOutputStream, header: Unit) {}
    def writeHeader(out: DataOutputStream, value: Long, header: Unit) {}
    def readHeader(in: DataInputStream) {}
    
    def writeRecord(out: DataOutputStream, value: Long, header: Unit) {
      out.writeLong(value)
    }
    
    def readRecord(in: DataInputStream, header: Unit) = in.readLong()
  }
  
  
  "iterable dataset ops" should {
    "cogroup" in {
      "for abitrary datasets" in {
        implicit val idCount = genFixedIdCount(1)
        check { (p: DSPair[Long]) => {
          val (l1, l2) = p
          
          implicit val idSort = IdentitiesOrder.toScalaOrdering

          val l1List = l1.iterable.toList.sorted
          val l2List = l2.iterable.toList.sorted

          type ResultList = List[Record[Either3[Long, (Long,Long), Long]]]

          val tOrder = tupledIdentitiesOrder[Long](IdentitiesOrder)

          @tailrec def computeCogroup(l: List[Record[Long]], r: List[Record[Long]], acc: ResultList): ResultList = {
            (l,r) match {
              case (lh :: lt, rh :: rt) => tOrder.order(lh,rh) match {
                case EQ => {
                  val (leftSpan, leftRemain) = l.partition(tOrder.order(_, lh) == EQ)
                  val (rightSpan, rightRemain) = r.partition(tOrder.order(_, rh) == EQ)

                  val cartesian = leftSpan.flatMap { lv => rightSpan.map { rv => (rv._1, middle3((lv._2,rv._2))) } }

                  computeCogroup(leftRemain, rightRemain, acc ::: cartesian)
                }
                case LT => {
                  val (leftRun, leftRemain) = l.partition(tOrder.order(_, rh) == LT)
                  
                  computeCogroup(leftRemain, r, acc ::: leftRun.map { case (i,v) => (i, left3(v)) })
                }
                case GT => {
                  val (rightRun, rightRemain) = r.partition(tOrder.order(lh, _) == GT)

                  computeCogroup(l, rightRemain, acc ::: rightRun.map { case (i,v) => (i, right3(v)) })
                }
              }
              case (Nil, _) => acc ::: r.map { case (i,v) => (i, right3(v)) }
              case (_, Nil) => acc ::: l.map { case (i,v) => (i, left3(v)) }
            }
          }

          val expected = computeCogroup(l1List, l2List, Nil)

          val result = IterableDataset(1, l1List.toIterable).cogroup(IterableDataset(1, l2List.toIterable)) {
            new CogroupF[Long, Long, Either3[Long, (Long, Long), Long]] {
              def left(i: Long) = left3(i)
              def both(i1: Long, i2: Long) = middle3((i1, i2))
              def right(i: Long) = right3(i)
            }
          }.iterable.toList

          result must containAllOf(expected).only.inOrder
        }}
      }

      "for a static full dataset" in {
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

      "for a static single pair dataset" in {
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

      val cgf = new CogroupF[Long, Long, Either3[Long, (Long, Long), Long]] {
        def left(i: Long) = left3(i)
        def both(i1: Long, i2: Long) = middle3((i1, i2))
        def right(i: Long) = right3(i)
      }

      "with an assertion on unequal identity widths" in {
        val d1 = IterableDataset[Long](0, List((VectorCase[Long](), 1l)))
        val d2 = IterableDataset[Long](1, List((VectorCase[Long](1l), 1l)))

        d1.cogroup(d2)(cgf) must throwAn[AssertionError]
      }

      "with an assertion on zero-width identities" in {
        val d1 = IterableDataset[Long](0, List((VectorCase[Long](), 1l)))
        val d2 = IterableDataset[Long](0, List((VectorCase[Long](), 2l)))

        d1.cogroup(d2)(cgf) must throwAn[AssertionError]
      }
    }

    "join" in {
      "for arbitrary datasets" in { 
        implicit val idCount = genVariableIdCount
        check { (l1: IterableDataset[Long], l2: IterableDataset[Long]) => {
          implicit val idSort = IdentitiesOrder.toScalaOrdering

          val sharedPrefixLen = l1.idCount min l2.idCount

          val l1List = l1.iterable.toList.sorted
          val l2List = l2.iterable.toList.sorted

          type ResultList = List[Record[(Long,Long)]]

          import scala.annotation.tailrec
          import scalaz.Ordering._

          def order(l: Record[Long], r: Record[Long]): Ordering = prefixIdentityOrdering(l._1, r._1, sharedPrefixLen)

          @tailrec def computeJoin(l: List[Record[Long]], r: List[Record[Long]], acc: ResultList): ResultList = {
            (l,r) match {
              case (lh :: lt, rh :: rt) => order(lh, rh) match {
                case EQ => {
                  val (leftSpan, leftRemain) = l.partition(order(_, lh) == EQ)
                  val (rightSpan, rightRemain) = r.partition(order(_, rh) == EQ)

                  val cartesian = leftSpan.flatMap { lv => rightSpan.map { rv => (lv._1 ++ rv._1.drop(sharedPrefixLen), (lv._2,rv._2)) } }

                  computeJoin(leftRemain, rightRemain, acc ::: cartesian)
                }
                case LT => computeJoin(l.dropWhile(order(_, rh) == LT), r, acc)
                case GT => computeJoin(l, r.dropWhile(order(lh, _) == GT), acc)
              }
              case (Nil, _) => acc
              case (_, Nil) => acc
            }
          }

          val expected = computeJoin(l1List, l2List, Nil)

          val result = IterableDataset(1, l1List.toIterable).join(IterableDataset(1, l2List.toIterable), sharedPrefixLen) {
            case (a, b) => (a, b)
          }.iterable.toList

          //result must containAllOf(expected).only.inOrder
          result must_== expected
        }}
      }

      "for a static full dataset" in {
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

      "for a static dataset with zero-width identities" in {
        val v1  = IterableDataset(1, Vector(rec(1), rec(3), rec(3), rec(5), rec(7), rec(8), rec(8)))
        val v2  = IterableDataset(0, Vector((VectorCase(), 3)))

        val results = v1.join(v2, 0) {
          case (a, b) => (a, b)
        }

        val expected = Vector(
          (1, 3),
          (3, 3),
          (3, 3),
          (5, 3),
          (7, 3),
          (8, 3),
          (8, 3)
        )

        Vector(results.iterator.toSeq: _*) must_== expected
      }
    }

    "crossLeft" in {
      implicit val idCount = genVariableIdCount
      check { (l1: IterableDataset[Long], l2: IterableDataset[Long]) => 
        val results = l1.crossLeft(l2) { 
          case (a, b) => (a, b)
        }
             
        results.iterator.toList must_== l1.iterator.toList.flatMap(i => l2.iterator.toList.map((j: Long) => (i, j)))
      } 
    }

    "crossRight" in {
      implicit val idCount = genVariableIdCount
      check { (l1: IterableDataset[Long], l2: IterableDataset[Long]) => 
        val results = l1.crossRight(l2) { 
          case (a, b) => (a, b)
        }

        results.iterator.toList must_== l2.iterator.toList.flatMap(i => l1.iterator.toList.map((j: Long) => (j, i)))
      } 
    }

    "paddedMerge" in {
      check { (sample: MergeSample) => 
        val results = sample.l1.paddedMerge(sample.l2, () => idSource.nextId()).iterator.toList
        val sampleLeft = sample.l1.iterator.toList
        val sampleRight = sample.l2.iterator.toList

        (results must haveSize(sampleLeft.size + sampleRight.size)) and
        (results must haveTheSameElementsAs(sampleLeft ++ sampleRight))
      }
    }
    
    "union" in {
      "for arbitrary datasets" in {
        implicit val ordering = identityValueOrder[Long](IdentitiesOrder).toScalaOrdering
        implicit val idCount = genVariableIdCount
  
        check { (p: DSPair[Long]) =>
          val (l1, l2) = p
          val results = l1.union(l2, memoCtx).iterable.toList
          val expectedSet = Set((l1.iterable ++ l2.iterable).toSeq: _*).toList
          val expectedSorted = expectedSet.sorted
  
          results must containAllOf(expectedSorted).only.inOrder
        }
      }

      "with an assertion on unequal identity widths" in {
        val d1 = IterableDataset[Long](0, List((VectorCase[Long](), 1l)))
        val d2 = IterableDataset[Long](1, List((VectorCase[Long](1l), 1l)))

        d1.union(d2, memoCtx) must throwAn[AssertionError]
      }
    }

    "intersect" in {
      "for arbitrary datasets" in {
        implicit val ordering = identityValueOrder[Long](IdentitiesOrder).toScalaOrdering
        implicit val idCount = genVariableIdCount
  
        check { (p: DSPair[Long]) =>
          val (l1, l2) = p
          val results = l1.intersect(l2, memoCtx).iterable.toList
          val expectedSet = (Set(l1.iterable.toSeq: _*) & Set(l2.iterable.toSeq: _*)).toList
          val expectedSorted = expectedSet.sorted
  
          results must containAllOf(expectedSorted).only.inOrder
        }
      }

      "with an assertion on unequal identity widths" in {
        val d1 = IterableDataset[Long](0, List((VectorCase[Long](), 1l)))
        val d2 = IterableDataset[Long](1, List((VectorCase[Long](1l), 1l)))

        d1.union(d2, memoCtx) must throwAn[AssertionError]
      }
    }

    "group according to an integer key" in {
      "for a static input" in {
        val groups = Stream(
          0L -> Vector(rec(1)),
          2L -> Vector(rec(2), rec(3), rec(3)),
          4L -> Vector(rec(5)),
          6L -> Vector(rec(6), rec(7)),
          8L -> Vector(rec(8)))
        
        val ds = IterableDataset(1, groups.unzip._2 reduce { _ ++ _ })
        
        val result = ds.group(IdGen.nextInt(), memoCtx) { num =>
          IterableDataset(1, Vector(rec((num / 2) * 2)))
        }

        val result2 = mapGrouping(result) { 
          case IterableDataset(count, str) => IterableDataset(count, Vector(str.toSeq: _*))
        }

        val expected = groups map {
          case (k, v) => (k, IterableDataset(1, v))
        }
        
        result2.iterator.toList must_== expected.toList
      }

      "for arbitrary inputs" in {
        implicit val idTupleOrder = com.precog.yggdrasil.tupledIdentitiesOrder[Long](IdentitiesOrder).toScalaOrdering
        implicit val idCount = genVariableIdCount

        check { (ds: IterableDataset[Long]) => (!ds.iterable.isEmpty) ==> {
          val expected = ds.iterable.groupBy(_._2 / 2 * 2).map { case (k,v) => (k, IterableDataset[Long](ds.idCount, Vector(v.toSeq: _*).sorted)) }.toList.sortBy(_._1)

          val result = ds.group(IdGen.nextInt(), memoCtx) {
            v => IterableDataset[Long](1, Vector(rec(v / 2 * 2)))
          }.iterator.toList.map {
            // Make the datasets wrap Vectors so that the types align with expected and specs doesn't have a fit
            case (k, IterableDataset(count, iterable)) => (k, IterableDataset(count, Vector(iterable.toSeq: _*)))
          }

          result must containAllOf(expected).only.inOrder
        } }
      }
    }
          
    
    "group according to an identity key" in {
      "for a static input" in {
        val groups = Stream(
          1L -> Vector(rec(1)),
          2L -> Vector(rec(2)),
          3L -> Vector(rec(3)),
          5L -> Vector(rec(5)),
          6L -> Vector(rec(6)),
          7L -> Vector(rec(7)),
          8L -> Vector(rec(8)))
        
        val ds = IterableDataset(1, groups.unzip._2 reduce { _ ++ _ })
        
        val result = ds.group(IdGen.nextInt(), memoCtx) { num =>
          IterableDataset(1, Vector(rec(num)))
        }
  
        val result2 = mapGrouping(result) { 
          case IterableDataset(count, str) => IterableDataset(count, Vector(str.toSeq: _*))
        }
        
        val expected = groups map {
          case (k, v) => (k, IterableDataset(1, v))
        }
        
        result2.iterator.toList must_== expected.toList
      }

      "for arbitrary inputs" in {
        implicit val idTupleOrder = com.precog.yggdrasil.tupledIdentitiesOrder[Long](IdentitiesOrder).toScalaOrdering
        implicit val idOrder = com.precog.yggdrasil.IdentitiesOrder.toScalaOrdering
        implicit val idCount = genVariableIdCount        
        
        check { (ds: IterableDataset[Long]) => {
          val expected: List[(Long,IterableDataset[Long])] = ds.iterable.groupBy(_._2).map { case (k,v) => (k, IterableDataset[Long](ds.idCount, Vector(v.toSeq: _*).sorted)) }.toList.sortBy(_._1)

          val result = ds.group(IdGen.nextInt(), memoCtx) {
            num => IterableDataset[Long](1, Vector(rec(num)))
          }.iterator.toList.map {
            // Make the datasets wrap Vectors so that the types align with expected and specs doesn't have a fit
            case (k, IterableDataset(count, iterable)) => (k, IterableDataset(count, Vector(iterable.toSeq: _*)))
          }

          result must containAllOf(expected).only.inOrder
        } }
      }
    }

    "sort by indexed identities" in {
      "for arbitrary inputs" >> {
        implicit val idCount: Gen[IdCount] = IdCount(5)
        check { (ds: IterableDataset[Long]) => 
          val expected = ds.iterable.toList sortWith {
            case ((ids1, _), (ids2, _)) => 
              if (ids1(2) == ids2(2)) {
                if (ids1(0) == ids2(0)) {
                  if (ids1(4) == ids2(4)) {
                    false
                  } else {
                    ids1(4) < ids2(4)
                  }
                } else {
                  ids1(0) < ids2(0)
                }
              } else {
                ids1(2) < ids2(2)
              }
          } map {
            case (_, v) => v
          }
          
          val result = ds.sortByIndexedIds(Vector(2, 0, 4), IdGen.nextInt(), memoCtx).iterator.toList

          result must containAllOf(expected).only.inOrder
        }
      }
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
      "for a reference grouping" in {
        g1.iterator.size must_== 5
        val result = mapGrouping(g1) { 
          v => v.iterable.size
        }
      
        result.iterator.toList must containAllOf(List(
          0L -> 1,
          2L -> 3,
          4L -> 1,
          6L -> 2,
          8L -> 1)).only.inOrder
      }

      "for arbitrary groupings" in {
        implicit val idTupleOrder = com.precog.yggdrasil.tupledIdentitiesOrder[Long](IdentitiesOrder).toScalaOrdering
        implicit val idCount = genVariableIdCount

        check { (ds: IterableDataset[Long]) => (!ds.iterable.isEmpty) ==> {
          def mapFunc(r: Record[Long]) = (r._1, r._2 * 2)
          
          val expected = ds.iterable.groupBy(_._2 % 10).map { case (k,v) => (k, IterableDataset[Long](ds.idCount, Vector(v.map(mapFunc).toSeq: _*).sorted)) }.toList.sortBy(_._1)

          val grouped = ds.group(IdGen.nextInt(), memoCtx) {
            v => IterableDataset[Long](1, Vector(rec(v % 10)))
          }

          val result = mapGrouping(grouped) {
            (a: IterableDataset[Long]) => IterableDataset[Long](a.idCount, a.iterable.map(mapFunc))
          }.iterator.toList

          result must containAllOf(expected).only.inOrder
        } }

      }
    }

    "implement flatten" in {
      implicit val groupFunc = (a: Long) => a % 3
      implicit val idCount = genVariableIdCount

      implicit val arbGroup = Arbitrary(groupingNelGen[Long,Long])

      check { (g1: IterableGrouping[Long, NEL[IterableDataset[Long]]]) => (g1.iterator.hasNext == true) ==> {
        val list1: List[(Long,NEL[IterableDataset[Long]])] = g1.iterator.toList

        def idGen() = {
          var id = -1l
          () => { id += 1; id }
        }

        val expIds = idGen()
        val flattenFunc: (Long,NEL[IterableDataset[Long]]) => IterableDataset[Long] = {
          case (k, nv) => if (k == 0) {
            // Force a discard to ensure we handle the case where a grouping transforms to "no results"
            // A bug in flattenGroup would cause the iteration to stop prematurely when this was the case
            IterableDataset(1, Iterable()) 
          } else {
            IterableDataset(1, Iterable.concat(nv.list.map(_.iterable): _*).map{ case (_,v) => (VectorCase(expIds()), v) })
          }
        }
        
        val ng1 = IterableGrouping(list1.iterator)
        implicit val ordering = Order[Long].toScalaOrdering

        val expected: List[Long] = Iterable.concat(list1.map { case (k,v) => flattenFunc(k, v).iterable map { case (id, v) => v } }: _*).toList

        val ids = idGen()
        import scalaz.std.tuple._
        import com.precog.util.IdGen
        val result: List[Long] = flattenGroup(ng1, ids, IdGen.nextInt(), memoCtx)(flattenFunc).iterator.toList

        result must containAllOf(expected).only
      }}
    }
    
    "implement merging" >> {
      "union" >> {
        val result = mergeGroups(g1, g2, true, memoCtx).iterator.map {
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
        val result = mergeGroups(g1, g2, false, memoCtx).iterator.map {
          case (k, v) => (k, IterableDataset(v.idCount, Vector(v.iterable.toSeq: _*)))
        }

        val expected = List(
          2L -> IterableDataset(1, Vector(sharedRecs('i2), sharedRecs('i3a))),
          8L -> IterableDataset(1, Vector()))
          
        result.toList mustEqual expected.toList
      }
    }
    
    "implement zipping" in {
      "for a reference grouping" in {
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

      "for arbitrary groupings" in {
        implicit val groupFunc = (a: Long) => a % 10
        implicit val idCount = genVariableIdCount

        implicit val arbGroup = Arbitrary(groupingGen[Long,Long]())

        check { (g1: IterableGrouping[Long, IterableDataset[Long]], g2: IterableGrouping[Long, IterableDataset[Long]]) => {
          val list1 = g1.iterator.toList
          val list2 = g2.iterator.toList
          
          @tailrec def zipList(l1: List[(Long, IterableDataset[Long])], l2: List[(Long, IterableDataset[Long])], acc: List[(Long,NEL[IterableDataset[Long]])]): List[(Long,NEL[IterableDataset[Long]])]  = (l1, l2) match {
            case (h1 :: t1, h2 :: t2) => {
              if (h1._1 == h2._1) {
                zipList(t1, t2, (h1._1, NEL(h1._2, h2._2)) :: acc)
              } else if (h1._1 < h2._1) {
                zipList(t1, l2, acc)
              } else {
                zipList(l1, t2, acc)
              }
            }
            case (Nil, _) => acc.reverse
            case (_, Nil) => acc.reverse
          }

          val ng1 = mapGrouping(IterableGrouping(list1.iterator)) { v => NEL(v) }
          val ng2 = mapGrouping(IterableGrouping(list2.iterator)) { v => NEL(v) }

          val result = zipGroups(ng1, ng2).iterator.toList

          val expected = zipList(list1, list2, Nil)

          result must containAllOf(expected).only.inOrder
        }}
      }
    } 
  }
}
