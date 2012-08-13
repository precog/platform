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

import scala.collection.immutable.BitSet
import scala.util.Random

import blueeyes.json._
import blueeyes.json.JsonAST._

import scalaz.StreamT
import scalaz.syntax.copointed._

import org.specs2.ScalaCheck
import org.specs2.mutable._

trait CompactSpec[M[+_]] extends TestColumnarTableModule[M] with TableModuleTestSupport[M] with Specification with ScalaCheck {
  import SampleData._
  import trans._
  
  def tableStats(table: Table) : List[(Int, Int)] = table match {
    case cTable: ColumnarTable => 
      val slices = cTable.slices.toStream.copoint
      val sizes = slices.map(_.size).toList
      val undefined = slices.map { slice =>
        (0 until slice.size).foldLeft(0) {
          case (acc, i) => if(!slice.columns.values.exists(_.isDefinedAt(i))) acc+1 else acc
        }
      }.toList

      sizes zip undefined
  }

  def undefineTable(fullTable: Table): Table = fullTable match {
    case cTable: ColumnarTable =>
      val slices = cTable.slices.toStream.copoint // fuzzing must be done strictly otherwise sadness will ensue
      val numSlices = slices.size
      
      val maskedSlices = slices.map { slice =>
        if(numSlices > 1 && Random.nextDouble < 0.25) {
          new Slice {
            val size = slice.size
            val columns = slice.columns.mapValues { col => (col |> cf.util.filter(0, slice.size, BitSet())).get }
          }
        } else {
          val retained = (0 until slice.size).map { (x : Int) => if(scala.util.Random.nextDouble < 0.75) Some(x) else None }.flatten
          new Slice {
            val size = slice.size 
            val columns = slice.columns.mapValues { col => (col |> cf.util.filter(0, slice.size, BitSet(retained: _*))).get }
          }
        }
      }
      
      table(StreamT.fromStream(M.point(maskedSlices)))
  }

  def testCompactIdentity = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val compactTable = table.compact(Leaf(Source))

      val results = toJson(compactTable)

      results.copoint must_== sample.data
    }
  }

  def testCompactPreserve = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val sampleTable = undefineTable(fromSample(sample))
      val sampleJson = toJson(sampleTable)
      
      val compactTable = sampleTable.compact(Leaf(Source))
      val results = toJson(compactTable)

      results.copoint must_== sampleJson.copoint
    }
  }

  def testCompactRows = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val sampleTable = undefineTable(fromSample(sample))
      val sampleJson = toJson(sampleTable)
      val sampleStats = tableStats(sampleTable)
      
      val compactTable = sampleTable.compact(Leaf(Source))
      val compactStats = tableStats(compactTable)
      val results = toJson(compactTable)

      compactStats.map(_._2).foldLeft(0)(_+_) must_== 0
    }
  }
    
  def testCompactSlices = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val sampleTable = undefineTable(fromSample(sample))
      val sampleJson = toJson(sampleTable)
      val sampleStats = tableStats(sampleTable)
      
      val compactTable = sampleTable.compact(Leaf(Source))
      val compactStats = tableStats(compactTable)
      val results = toJson(compactTable)

      compactStats.map(_._1).count(_ == 0) must_== 0
    }
  }
}
