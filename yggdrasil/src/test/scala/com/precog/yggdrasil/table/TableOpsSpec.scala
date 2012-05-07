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
package table

import com.precog.common.Path
import com.precog.common.VectorCase

import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._

import scala.annotation.tailrec
import scalaz._

import org.specs2._
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

class TableOpsSpec extends DatasetOpsSpec { spec =>
  override val defaultPrettyParams = Pretty.Params(2)

  val sliceSize = 10
  val testPath = Path("/tableOpsSpec")

  def slice(sampleData: SampleData): (Slice, SampleData) = {
    val (prefix, suffix) = sampleData.data.splitAt(sliceSize)
    var i = 0
    val (ids, columns) = prefix.foldLeft((List.fill(sampleData.idCount)(ArrayColumn(CLong, sliceSize)), Map.empty[VColumnRef[_], Column[_]])) {
      case ((idsAcc, colAcc), (ids, jv)) =>
        for (j <- 0 until ids.length) idsAcc(j)(i) = ids(j)

        val newAcc = jv.flattenWithPath.foldLeft(colAcc) {
          case (acc, (jpath, v)) =>
            val ctype = CType.forJValue(v).get
            val ref = VColumnRef[ctype.CA](NamedColumnId(testPath, jpath), ctype)

            val col: Column[_] = v match {
              case JString(s) => 
                val col: ArrayColumn[String] = acc.getOrElse(ref, ArrayColumn(CStringArbitrary, sliceSize)).asInstanceOf[ArrayColumn[String]]
                col(i) = s
                col
              
              case JInt(ji) => CType.sizedIntCValue(ji) match {
                case CInt(v) => 
                  val col: ArrayColumn[Int] = acc.getOrElse(ref, ArrayColumn(CInt, sliceSize)).asInstanceOf[ArrayColumn[Int]]
                  col(i) = v
                  col

                case CLong(v) =>
                  val col: ArrayColumn[Long] = acc.getOrElse(ref, ArrayColumn(CLong, sliceSize)).asInstanceOf[ArrayColumn[Long]]
                  col(i) = v
                  col

                case CNum(v) =>
                  val col: ArrayColumn[BigDecimal] = acc.getOrElse(ref, ArrayColumn(CDecimalArbitrary, sliceSize)).asInstanceOf[ArrayColumn[BigDecimal]]
                  col(i) = v
                  col
              }

              case JDouble(d) => 
                val col: ArrayColumn[Double] = acc.getOrElse(ref, ArrayColumn(CDouble, sliceSize)).asInstanceOf[ArrayColumn[Double]]
                col(i) = d
                col

              case JBool(b) => 
                val col: ArrayColumn[Boolean] = acc.getOrElse(ref, ArrayColumn(CBoolean, sliceSize)).asInstanceOf[ArrayColumn[Boolean]]
                col(i) = b
                col

              case JArray(Nil)  => 
                val col = acc.getOrElse(ref, new CEmptyArrayColumn(sliceSize)).asInstanceOf[NullColumn]
                col.defined(i) = true
                col

              case JObject(Nil) => 
                val col = acc.getOrElse(ref, new CEmptyObjectColumn(sliceSize)).asInstanceOf[NullColumn]
                col.defined(i) = true
                col

              case JNull        => 
                val col = acc.getOrElse(ref, new CNullColumn(sliceSize)).asInstanceOf[NullColumn]
                col.defined(i) = true
                col
            }

            acc + (ref -> col)
        }

        i += 1

        (idsAcc, newAcc)
    }

    val result = Slice(ids, columns mapValues { case c: ArrayColumn[_] => c.prefix(i); case x => x }, i)

    (result, SampleData(sampleData.idCount, suffix))
  }

  def fromJson(sampleData: SampleData): Table = {
    val (s, xs) = spec.slice(sampleData)

    new Table(sampleData.idCount, s.columns.keySet, new Iterable[Slice] {
      def iterator = new Iterator[Slice] {
        private var _next = s
        private var _rest = xs

        def hasNext = _next != null
        def next() = {
          val tmp = _next
          _next = if (_rest.data.isEmpty) null else {
            val (s, xs) = spec.slice(_rest)
            _rest = xs
            s
          }
          tmp
        }
      }
    })
  }

  def toJson(dataset: Table): Stream[Record[JValue]] = {
    dataset.toEvents.toStream
  }

  def toValidatedJson(dataset: Table): Stream[Record[ValidationNEL[Throwable, JValue]]] = {
    dataset.toValidatedEvents.toStream
  }

  "a table dataset" should {
    "verify bijection from static JSON" in {
      val sample: List[(Identities, JValue)] = List(
        (VectorCase(-1l, 0l),JNull), 
        (VectorCase(-3090012080927607325l, 2875286661755661474l),
          JObject(List(
            JField("q8b", JArray(List(
              JDouble(6.615224799778253E307d), 
              JArray(List(JBool(false), JNull, JDouble(-8.988465674311579E307d))), JDouble(-3.536399224770604E307d)))), 
            JField("lwu",JDouble(-5.121099465699862E307d))))), 
        (VectorCase(-3918416808128018609l, 1l),JDouble(-1.0))
      )

      val dataset = fromJson(SampleData(2, sample.toStream))
      val results = dataset.toEvents.toList
      results must containAllOf(sample).only
    }

    "verify bijection from JSON" in checkMappings
    "in cogroup" >> {
      //"survive pathology1" in testCogroupPathology1
      /*
      "survive pathology2" in {
        skipped
        testCogroupPathology2
      }
      */
      //"survive pathology3" in testCogroupPathology3
      //"survive pathology4" in testCogroupPathology4
      "survive pathology5" in testCogroupPathology5
      //"survive scalacheck" in { check { (l: SampleData, r: SampleData) => testCogroup(l, r) } }
    }
  }
}


// vim: set ts=4 sw=4 et:
