package com.precog.yggdrasil

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

import scalaz.Either._

trait DatasetExtensionsSpec extends Specification with ScalaCheck {
  type Dataset = Table
  type Record[A] = (Identities, A)

  def fromJson(jv: Stream[Record[JValue]]): Dataset
  def toJson(dataset: Dataset): Stream[Record[JValue]]

  def checkCogroup = {
    type CogroupResult[A] = Stream[Record[Either3[A, (A, A), A]]]

    @tailrec def computeCogroup[A](l: Stream[Record[A], r: Stream[Record[A]], acc: CogroupResult[A])(implicit ord: Order[Record[A]]): CogroupResult[A] = {
      (l,r) match {
        case (lh ::# lt, rh ::# rt) => ord.order(lh, rh) match {
          case EQ => {
            val (leftSpan, leftRemain) = l.partition(ord.order(_, lh) == EQ)
            val (rightSpan, rightRemain) = r.partition(ord.order(_, rh) == EQ)

            val cartesian = leftSpan.flatMap { lv => rightSpan.map { rv => (rv._1, middle3((lv._2,rv._2))) } }

            computeCogroup(leftRemain, rightRemain, acc ++ cartesian)
          }
          case LT => {
            val (leftRun, leftRemain) = l.partition(ord.order(_, rh) == LT)
            
            computeCogroup(leftRemain, r, acc ++ leftRun.map { case (i,v) => (i, left3(v)) })
          }
          case GT => {
            val (rightRun, rightRemain) = r.partition(ord.order(lh, _) == GT)

            computeCogroup(l, rightRemain, acc ++ rightRun.map { case (i,v) => (i, right3(v)) })
          }
        }
        case (Nil, _) => acc ++ r.map { case (i,v) => (i, right3(v)) }
        case (_, Nil) => acc ++ l.map { case (i,v) => (i, left3(v)) }
      }
    }

    check { (l: Stream[Record[JValue]], r: Stream[Record[JValue]]) =>
      val expected = computeCogroup(l, r, Stream())
      val result = toJson(fromJson(l).cogroup(fromJson(r))(CogroupMerge.second))

      result must containAllOf(expected).only.inOrder
    }
  }
}

// vim: set ts=4 sw=4 et:
