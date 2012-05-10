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
import com.precog.common.VectorCase
import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser

import scalaz.{NonEmptyList => NEL, _}
import scalaz.Ordering._
import scalaz.Either3._
import scalaz.std.tuple._
import scalaz.std.function._
import scalaz.syntax.arrow._
import scala.annotation.tailrec

import org.specs2._
import org.specs2.mutable.Specification
import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

trait DatasetOpsSpec extends Specification with ScalaCheck with CValueGenerators {
  type Dataset = Table
  type Record[A] = (Identities, A)

  override val defaultPrettyParams = Pretty.Params(2)

  implicit def order[A] = tupledIdentitiesOrder[A]()

  case class SampleData(idCount: Int, data: Stream[Record[JValue]]) {
    override def toString = {
      "\nSampleData: \nidCount = "+idCount+",\ndata = "+
      data.map({ case (ids, v) => ids.mkString("(", ",", ")") + ": " + v.toString.replaceAll("\n", "\n  ") }).mkString("[\n  ", ",\n  ", "]\n")
    }
  }

  def fromJson(sampleData: SampleData): Dataset
  def toJson(dataset: Dataset): Stream[Record[JValue]]
  def toValidatedJson(dataset: Dataset): Stream[Record[ValidationNEL[Throwable, JValue]]]

  def normalizeValidations(s: Stream[Record[ValidationNEL[Throwable, JValue]]]): Stream[Record[Option[JValue]]] = {
    s map {
      case (ids, Failure(t)) => t.list.foreach(_.printStackTrace); (ids, None)
      case (ids, Success(v)) => (ids, Some(v))
    }
  }

  implicit def identitiesOrdering = IdentitiesOrder.toScalaOrdering

  implicit val arbData = Arbitrary(
    for {
      depth   <- choose(0, 3)
      (idCount, data) <- genEventColumns(schema(depth))
    } yield {
      SampleData(idCount, data.sortBy(_._1).toStream map { (assemble _).second })
    }
  )

  implicit val cogroupData = Arbitrary(
    for {
      depth   <- choose(1, 2)
      (idCount, data) <- genEventColumns(Gen.oneOf(arraySchema(depth, 2), objectSchema(depth, 2)))
    } yield {
      val (l, r) =  data map {
                      case (ids, values) => 
                        val (d1, d2) = values.splitAt(values.length/2)
                        (ids, assemble(d1), assemble(d2))
                    } map {
                      case (ids, v1, v2) => ((ids, v1), (ids, v2))
                    } unzip

      (SampleData(idCount, l.sortBy(_._1).toStream), SampleData(idCount, r.sortBy(_._1).toStream))
    }
  )

  def checkMappings = {
    check { (sample: SampleData) =>
      val dataset = fromJson(sample)
      dataset.toEvents.toList must containAllOf(sample.data.toList).only
    }
  }

  type CogroupResult[A] = Stream[Record[Either3[A, (A, A), A]]]
  @tailrec protected final def computeCogroup[A](l: Stream[Record[A]], r: Stream[Record[A]], acc: CogroupResult[A], idPrefix: Int)(implicit ord: Order[Record[A]]): CogroupResult[A] = {
    (l, r) match {
      case (lh #:: lt, rh #:: rt) => ord.order(lh, rh) match {
        case EQ => {
          val (leftSpan, leftRemain) = l.partition(ord.order(_, lh) == EQ)
          val (rightSpan, rightRemain) = r.partition(ord.order(_, rh) == EQ)

          val cartesian = leftSpan.flatMap { case (idl, lv) => rightSpan.map { case (idr, rv) => (idl ++ idr.drop(idPrefix), middle3((lv, rv))) } }

          computeCogroup(leftRemain, rightRemain, acc ++ cartesian, idPrefix)
        }
        case LT => {
          val (leftRun, leftRemain) = l.partition(ord.order(_, rh) == LT)
          
          computeCogroup(leftRemain, r, acc ++ leftRun.map { case (i, v) => (i, left3(v)) }, idPrefix)
        }
        case GT => {
          val (rightRun, rightRemain) = r.partition(ord.order(lh, _) == GT)

          computeCogroup(l, rightRemain, acc ++ rightRun.map { case (i, v) => (i, right3(v)) }, idPrefix)
        }
      }
      case (Stream.Empty, _) => acc ++ r.map { case (i,v) => (i, right3(v)) }
      case (_, Stream.Empty) => acc ++ l.map { case (i,v) => (i, left3(v)) }
    }
  }

  def testCogroup(l: SampleData, r: SampleData) = {
    val idCount = l.idCount max r.idCount

    val ltable = fromJson(l)
    val rtable = fromJson(r)

    val expected = computeCogroup(l.data, r.data, Stream(), l.idCount min r.idCount) map {
      case (ids, Left3(jv)) => (ids, jv)
      case (ids, Middle3((jv1, jv2))) => (ids, jv1.insertAll(jv2) match { case Success(v) => v; case Failure(ts) => throw ts.head })
      case (ids, Right3(jv)) => (ids, jv)
    } map {
      case (ids, jv) => (VectorCase(ids.padTo(idCount, -1l): _*), jv)
    }

    val result = toJson(ltable.cogroup(rtable, ltable.idCount min rtable.idCount)(CogroupMerge.second))
    result must containAllOf(expected).only

//    val expected = computeCogroup(l.data, r.data, Stream(), l.idCount min r.idCount) map {
//      case (ids, Left3(jv)) => (ids, Validation.success[NEL[Throwable], JValue](jv))
//      case (ids, Middle3((jv1, jv2))) => (ids, jv1.insertAll(jv2))
//      case (ids, Right3(jv)) => (ids, Validation.success[NEL[Throwable], JValue](jv))
//    } map {
//      case (ids, jv) => (VectorCase(ids.padTo(idCount, -1l): _*), jv)
//    }
//
//    val result = toValidatedJson(ltable.cogroup(rtable, ltable.idCount min rtable.idCount)(CogroupMerge.second))
//    normalizeValidations(result) must containAllOf(normalizeValidations(expected)).only.inOrder
  }
}

// vim: set ts=4 sw=4 et:
