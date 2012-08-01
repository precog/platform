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

import akka.dispatch.Future
import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser
import blueeyes.concurrent.test.FutureMatchers

import scalaz.{Ordering => _, NonEmptyList => NEL, _}
import scalaz.std.tuple._
import scalaz.std.function._
import scalaz.syntax.arrow._
import scalaz.syntax.bifunctor._
import scalaz.syntax.copointed._
import scala.annotation.tailrec

import org.specs2._
import org.specs2.mutable.Specification
import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

trait TableModuleSpec[M[+_]] extends Specification with ScalaCheck with CValueGenerators with TableModule[M] {
  import trans.constants._

  override val defaultPrettyParams = Pretty.Params(2)
  implicit def coM : Copointed[M]

  def lookupF1(namespace: List[String], name: String): F1
  def lookupF2(namespace: List[String], name: String): F2
  def lookupScanner(namespace: List[String], name: String): Scanner
  
  def fromJson(data: Stream[JValue], maxBlockSize: Option[Int] = None): Table
  def toJson(dataset: Table): M[Stream[JValue]] = dataset.toJson.map(_.toStream)

  def fromSample(sampleData: SampleData, maxBlockSize: Option[Int] = None): Table = fromJson(sampleData.data, maxBlockSize)

  def debugPrint(dataset: Table): Unit 

  implicit def keyOrder[A]: scala.math.Ordering[(Identities, A)] = tupledIdentitiesOrder[A](IdentitiesOrder).toScalaOrdering

  def toRecord(ids: VectorCase[Long], jv: JValue): JValue = {
    JObject(Nil).set(Key, JArray(ids.map(JInt(_)).toList)).set(Value, jv)
  }

  def sample(schema: Int => Gen[JSchema]) = Arbitrary(
    for {
      depth   <- choose(0, 3)
      jschema <- schema(depth)
      (idCount, data) <- genEventColumns(jschema)
    } yield {
      try {
      
      SampleData(
        data.sorted.toStream flatMap {
          // Sometimes the assembly process will generate overlapping values which will
          // cause RuntimeExceptions in JValue.unsafeInsert. It's easier to filter these
          // out here than prevent it from happening in the first place.
          case (ids, jv) => try { Some(toRecord(ids, assemble(jv))) } catch { case _ : RuntimeException => None }
        },
        Some(jschema)
      )
      } catch {
        case ex => println("depth: "+depth) ; throw ex
      }
    }
  )

  case class SampleData(data: Stream[JValue], schema: Option[JSchema] = None) {
    override def toString = {
      "\nSampleData: ndata = "+data.map(_.toString.replaceAll("\n", "\n  ")).mkString("[\n  ", ",\n  ", "]\n")
    }
  }

  def checkMappings = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val dataset = fromSample(sample)
      toJson(dataset).copoint must containAllOf(sample.data.toList).only
    }
  }
}

// vim: set ts=4 sw=4 et:
