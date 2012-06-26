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
import scalaz.BiFunctor
import scalaz.Ordering._
import scalaz.Either3._
import scalaz.std.tuple._
import scalaz.std.function._
import scalaz.syntax.arrow._
import scalaz.syntax.biFunctor._
import scala.annotation.tailrec

import org.specs2._
import org.specs2.mutable.Specification
import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

trait TableModuleSpec extends Specification with ScalaCheck with CValueGenerators with TableModule {
  type Record[A] = (Identities, A)

  override val defaultPrettyParams = Pretty.Params(2)

  implicit def order[A] = tupledIdentitiesOrder[A]()

  case class SampleData(idCount: Int, data: Stream[Record[JValue]]) {
    override def toString = {
      "\nSampleData: \nidCount = "+idCount+",\ndata = "+
      data.map({ case (ids, v) => ids.mkString("(", ",", ")") + ": " + v.toString.replaceAll("\n", "\n  ") }).mkString("[\n  ", ",\n  ", "]\n")
    }
  }

  def fromJson(sampleData: SampleData): Table
  def toJson(dataset: Table): Stream[Record[JValue]]

  def toValidatedJson(dataset: Table): Stream[Record[ValidationNEL[Throwable, JValue]]]
  def debugPrint(dataset: Table): Unit 

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
      jschema <- schema(depth)
      (idCount, data) <- genEventColumns(jschema)
    } yield {
      SampleData(idCount, data.sortBy(_._1).toStream map { (assemble _).second })
    }
  )

  def checkMappings = {
    check { (sample: SampleData) =>
      val dataset = fromJson(sample)
      toJson(dataset).toList must containAllOf(sample.data.toList).only
    }
  }
}

// vim: set ts=4 sw=4 et:
