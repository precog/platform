package com.precog.yggdrasil

import table._
import blueeyes.json.JsonAST._

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

import scalaz._
import scalaz.Ordering._
import scalaz.Either3._
import scalaz.std.tuple._
import scala.annotation.tailrec

trait DatasetOpsSpec extends Specification with ScalaCheck with SValueGenerators {
  type Dataset = Table
  type Record[A] = (Identities, A)

  implicit def order[A] = tupledIdentitiesOrder[A]()

  def fromJson(jv: Stream[Record[JValue]]): Dataset
  def toJson(dataset: Dataset): Stream[Record[JValue]]

  def checkCogroup = {
    type CogroupResult[A] = Stream[Record[Either3[A, (A, A), A]]]
    implicit val arbRecords = Arbitrary(containerOf[Stream, Record[JValue]](sevent(2, 3) map { case (ids, sv) => (ids, sv.toJValue) }))

    @tailrec def computeCogroup[A](l: Stream[Record[A]], r: Stream[Record[A]], acc: CogroupResult[A])(implicit ord: Order[Record[A]]): CogroupResult[A] = {
      (l,r) match {
        case (lh #:: lt, rh #:: rt) => ord.order(lh, rh) match {
          case EQ => {
            val (leftSpan, leftRemain) = l.partition(ord.order(_, lh) == EQ)
            val (rightSpan, rightRemain) = r.partition(ord.order(_, rh) == EQ)

            val cartesian = leftSpan.flatMap { case (_, lv) => rightSpan.map { case (ids, rv) => (ids, middle3((lv, rv))) } }

            computeCogroup(leftRemain, rightRemain, acc ++ cartesian)
          }
          case LT => {
            val (leftRun, leftRemain) = l.partition(ord.order(_, rh) == LT)
            
            computeCogroup(leftRemain, r, acc ++ leftRun.map { case (i, v) => (i, left3(v)) })
          }
          case GT => {
            val (rightRun, rightRemain) = r.partition(ord.order(lh, _) == GT)

            computeCogroup(l, rightRemain, acc ++ rightRun.map { case (i, v) => (i, right3(v)) })
          }
        }
        case (Stream.Empty, _) => acc ++ r.map { case (i,v) => (i, right3(v)) }
        case (_, Stream.Empty) => acc ++ l.map { case (i,v) => (i, left3(v)) }
      }
    }

    check { (l: Stream[Record[JValue]], r: Stream[Record[JValue]]) =>
      val expected = computeCogroup(l, r, Stream()) map {
        case (ids, Left3(jv)) => (ids, jv)
        case (ids, Middle3((jv1, jv2))) => (ids, jv1 ++ jv2)
        case (ids, Right3(jv)) => (ids, jv)
      }

      val result = toJson(fromJson(l).cogroup(fromJson(r), 1)(CogroupMerge.second))

      result must containAllOf(expected).only.inOrder
    }
  }
}

// vim: set ts=4 sw=4 et:
