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

import table._
import blueeyes.json.JsonAST._

import scalaz._
import scalaz.Ordering._
import scalaz.Either3._
import scalaz.std.tuple._
import scala.annotation.tailrec

import org.specs2._
import org.specs2.mutable.Specification
import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

trait DatasetOpsSpec extends Specification with ScalaCheck with SValueGenerators {
  type Dataset = Table
  type Record[A] = (Identities, A)

  implicit def order[A] = tupledIdentitiesOrder[A]()

  case class SampleData(idCount: Int, data: Stream[Record[JValue]])

  def fromJson(sampleData: SampleData): Dataset
  def toJson(dataset: Dataset): Stream[Record[JValue]]

  def checkCogroup = {
    type CogroupResult[A] = Stream[Record[Either3[A, (A, A), A]]]
    implicit val arbData = Arbitrary(
      for {
        idCount <- choose(0, 3) 
        data <- containerOf[Stream, Record[JValue]](sevent(idCount, 3) map { case (ids, sv) => (ids, sv.toJValue) })
      } yield {
        SampleData(idCount, data)
      }
    )

    @tailrec def computeCogroup[A](l: Stream[Record[A]], r: Stream[Record[A]], acc: CogroupResult[A])(implicit ord: Order[Record[A]]): CogroupResult[A] = {
      (l, r) match {
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

    check { (l: SampleData, r: SampleData) =>
      try {
        val expected = computeCogroup(l.data, r.data, Stream()) map {
          case (ids, Left3(jv)) => (ids, jv)
          case (ids, Middle3((jv1, jv2))) => (ids, jv1 ++ jv2)
          case (ids, Right3(jv)) => (ids, jv)
        }

        val ltable = fromJson(l)
        println(l.data.toList)
        println(ltable.toEvents.toList)
        val rtable = fromJson(r)
        println(r.data.toList)
        println(rtable.toEvents.toList)
        val result = toJson(ltable.cogroup(rtable, 1)(CogroupMerge.second))

        result must containAllOf(expected).only.inOrder
      } catch {
        case ex => ex.printStackTrace; throw ex
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
