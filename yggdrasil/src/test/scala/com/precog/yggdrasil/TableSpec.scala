package com.precog.yggdrasil

import blueeyes.json.JsonAST._
import com.precog.common.VectorCase

import org.specs2.mutable._
import org.specs2.matcher.MatchResult
import scalaz._
import scalaz.std.option
import scalaz.syntax.bind._
import scalaz.syntax.std.optionV._
import scalaz.Either3._

import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary

object TableSpec extends Specification with ArbitraryProjectionDescriptor with ArbitrarySlice {
  "a table" should {
    "cogroup" in {
      /*
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
      */

      "a static full dataset" >> {
        val r1 = VColumnRef(DynColumnId(0), CLong)
        val v1 = new Table(
          1, Set(r1),
          List(
            new ArraySlice(
              VectorCase(Array(0L, 1L, 3L, 3L, 5L, 7L, 8L, 8L)),
              Map(r1 -> Array(0L, 1L, 3L, 3L, 5L, 7L, 8L, 8L))
            )
          )
        )

        val r2 = VColumnRef(DynColumnId(1), CLong)
        val v2 = new Table(
          1, Set(r2), 
          List(
            new ArraySlice(
              VectorCase(Array(0L, 2L, 3L, 4L, 5L, 5L, 6L, 8L, 8L)),
              Map(r2 -> Array(0L, 2L, 3L, 4L, 5L, 5L, 6L, 8L, 8L))
            )
          )
        )

        val expected = Vector(
          middle3((0L, 0L)),
          left3(1L),
          right3(2L),
          middle3((3L, 3L)),
          middle3((3L, 3L)),
          right3(4L),
          middle3((5L, 5L)),
          middle3((5L, 5L)),
          right3(6L),
          left3(7L),
          middle3((8L, 8L)),
          middle3((8L, 8L)),
          middle3((8L, 8L)),
          middle3((8L, 8L)) 
        )

        val results = v1.cogroup(v2)(CogroupMerge.second)
        val slice = results.slices.iterator.next

        expected.zipWithIndex.foldLeft(ok: MatchResult[Any]) {
          case (result, (Left3(v), i)) =>
            result and 
            (slice.column(VColumnRef(DynColumnId(0), CLong)).get.isDefinedAt(i) must beTrue) and
            (slice.column(VColumnRef(DynColumnId(0), CLong)).get.apply(i) must_== v)

          case (result, (Middle3((l, r)), i)) =>
            result and 
            (slice.column(VColumnRef(DynColumnId(0), CLong)).get.isDefinedAt(i) must beTrue) and
            (slice.column(VColumnRef(DynColumnId(0), CLong)).get.apply(i) must_== l) and
            (slice.column(VColumnRef(DynColumnId(1), CLong)).get.isDefinedAt(i) must beTrue) and
            (slice.column(VColumnRef(DynColumnId(1), CLong)).get.apply(i) must_== r) 

          case (result, (Right3(v), i)) =>
            result and 
            (slice.column(VColumnRef(DynColumnId(1), CLong)).get.isDefinedAt(i) must beTrue) and
            (slice.column(VColumnRef(DynColumnId(1), CLong)).get.apply(i) must_== v) 
        }
      }

      /*
      "perform" in {
        implicit val vm = Validation.validationMonad[String]
        val descriptor = genProjectionDescriptor.sample.get

        val slices1 = listOf(genSlice(descriptor, 10000)).sample.get
        val slices2 = listOf(genSlice(descriptor, 10000)).sample.get

        val table1 = new Table(slices1.head.idCount, slices1.head.columns.keySet, slices1)
        val table2 = new Table(slices2.head.idCount, slices2.head.columns.keySet, slices2)

        val startTime = System.currentTimeMillis
        val resultTable = table1.cogroup(table2)(CogroupMerge.second)

        resultTable.toJson.size
        val elapsed = System.currentTimeMillis - startTime
        println(elapsed)
        elapsed must beGreaterThan(0L)
      }

      "a static single pair dataset" in {
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

      */
    }
  }
}
