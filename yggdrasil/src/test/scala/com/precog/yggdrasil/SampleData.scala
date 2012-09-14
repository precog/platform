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

import com.precog.common.VectorCase

import akka.dispatch.Future
import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser
import blueeyes.json.xschema.DefaultOrderings.JValueOrdering
import blueeyes.concurrent.test.FutureMatchers

import scalaz.{Ordering => _, NonEmptyList => NEL, _}
import scalaz.std.tuple._
import scalaz.std.function._
import scalaz.syntax.arrow._
import scalaz.syntax.bifunctor._
import scalaz.syntax.copointed._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.generic.CanBuildFrom
import scala.util.Random

import org.specs2._
import org.specs2.mutable.Specification
import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import CValueGenerators.JSchema

case class SampleData(data: Stream[JValue], schema: Option[(Int, JSchema)] = None) {
  override def toString = {
    "SampleData: \ndata = "+data.map(_.toString.replaceAll("\n", "\n  ")).mkString("[\n  ", ",\n  ", "]\n") + 
    "\nschema: " + schema
  }

  def sortBy[B: Ordering](f: JValue => B) = copy(data = data.sortBy(f))
}

object SampleData extends CValueGenerators {
  def toRecord(ids: VectorCase[Long], jv: JValue): JValue = {
    JObject(Nil).set(JPath(".key"), JArray(ids.map(JNum(_)).toList)).set(JPath(".value"), jv)
  }

  implicit def keyOrder[A]: scala.math.Ordering[(Identities, A)] = tupledIdentitiesOrder[A](IdentitiesOrder).toScalaOrdering

  def sample(schema: Int => Gen[JSchema]) = Arbitrary(
    for {
      depth   <- choose(0, 1)
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
        Some((idCount, jschema))
      )
      } catch {
        case ex => println("depth: "+depth) ; throw ex
      }
    }
  )
  
  def distinctBy[T, C[X] <: Seq[X], S](c: C[T])(key: T => S)(implicit cbf: CanBuildFrom[C[T], T, C[T]]): C[T] = {
    val builder = cbf()
    val seen = mutable.HashSet[S]()
    
    for (t <- c) {
      if (!seen(key(t))) {
        builder += t
        seen += key(t)
      }
    }
    
    builder.result
  }
  
  def randomSubset[T, C[X] <: Seq[X], S](c: C[T], freq: Double)(implicit cbf: CanBuildFrom[C[T], T, C[T]]): C[T] = {
    val builder = cbf()
    
    for (t <- c)
      if (Random.nextDouble < freq)
        builder += t
    
    builder.result
  }
  
  def sort(sample: Arbitrary[SampleData]): Arbitrary[SampleData] = {
    Arbitrary(
      for {
        sampleData <- arbitrary(sample)
      } yield {
        SampleData(sampleData.data.sorted, sampleData.schema)
      }
    )
  }
  
  def shuffle(sample: Arbitrary[SampleData]): Arbitrary[SampleData] = {
    val gen =
      for {
        sampleData <- arbitrary(sample)
      } yield {
        SampleData(Random.shuffle(sampleData.data), sampleData.schema)
      }
    
    Arbitrary(gen)
  }

  def distinct(sample: Arbitrary[SampleData]) : Arbitrary[SampleData] = {
    Arbitrary(
      for {
        sampleData <- arbitrary(sample)
      } yield {
        SampleData(sampleData.data.distinct, sampleData.schema)
      }
    )
  }

  def distinctKeys(sample: Arbitrary[SampleData]) : Arbitrary[SampleData] = {
    Arbitrary(
      for {
        sampleData <- arbitrary(sample)
      } yield {
        SampleData(distinctBy(sampleData.data)(_ \ "keys"), sampleData.schema)
      }
    )
  }

  def distinctValues(sample: Arbitrary[SampleData]) : Arbitrary[SampleData] = {
    Arbitrary(
      for {
        sampleData <- arbitrary(sample)
      } yield {
        SampleData(distinctBy(sampleData.data)(_ \ "value"), sampleData.schema)
      }
    )
  }

  def duplicateRows(sample: Arbitrary[SampleData]): Arbitrary[SampleData] = {
    val gen =
      for {
        sampleData <- arbitrary(sample)
      } yield {
        val rows = sampleData.data
        val duplicates = randomSubset(rows, 0.25)
        SampleData(Random.shuffle(rows ++ duplicates), sampleData.schema)
      }
    
    Arbitrary(gen)
  }

  def undefineRows(sample: Arbitrary[SampleData]): Arbitrary[SampleData] = {
    val gen =
      for {
        sampleData <- arbitrary(sample)
      } yield {
        val rows = for(row <- sampleData.data)
          yield if (Random.nextDouble < 0.25) JNothing else row
        SampleData(rows, sampleData.schema) 
      }
    
    Arbitrary(gen)
  }

  def undefineRowsForColumn(sample: Arbitrary[SampleData], path: JPath): Arbitrary[SampleData] = {
    val gen =
      for {
        sampleData <- arbitrary(sample)
      } yield {
        val rows = for (row <- sampleData.data)
          yield if (Random.nextDouble < 0.25 && row.get(path) != JNothing) row.set(path, JNothing) else row
        SampleData(rows, sampleData.schema) 
      }
    
    Arbitrary(gen)
  }
}



// vim: set ts=4 sw=4 et:
