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
import com.precog.bytecode.JType
import com.precog.yggdrasil.util._

import akka.actor.ActorSystem
import akka.dispatch._
import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import com.weiglewilczek.slf4s.Logging

import scala.annotation.tailrec
import scala.collection.BitSet
import scala.collection.mutable.LinkedHashSet
import scala.util.Random

import scalaz._
import scalaz.effect.IO 
import scalaz.syntax.copointed._
import scalaz.std.anyVal._
import scalaz.std.stream._

import org.specs2._
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

import TableModule._

trait ColumnarTableModuleSpec[M[+_]] extends ColumnarTableModuleTestSupport[M] 
    with TableModuleSpec[M]
    with CogroupSpec[M]
    with CrossSpec[M]
    with TransformSpec[M]
    with CompactSpec[M] 
    with PartitionMergeSpec[M]
    with UnionAllSpec[M]
    with CrossAllSpec[M]
    with DistinctSpec[M] 
    with GroupingGraphSpec[M]
    { spec => 

  //type GroupId = Int
  import trans._
  import constants._
    
  override val defaultPrettyParams = Pretty.Params(2)

  private val groupId = new java.util.concurrent.atomic.AtomicInteger
  def newGroupId = groupId.getAndIncrement

  class MemoContext extends MemoizationContext {
    import trans._
    
    def memoize(table: Table, memoId: MemoId): M[Table] = M.point(table)
    def sort(table: Table, sortKey: TransSpec1, sortOrder: DesiredSortOrder, memoId: MemoId, unique: Boolean = true): M[Table] =
      table.sort(sortKey, sortOrder)
    
    def expire(memoId: MemoId): Unit = ()
    def purge(): Unit = ()
  }

  class Table(slices: StreamT[M, Slice]) extends ColumnarTable(slices) {
    import trans._
    def load(uid: UserId, jtpe: JType): M[Table] = sys.error("todo")
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean = true) = sys.error("todo")
    def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder = SortAscending, unique: Boolean = true): M[Seq[Table]] = sys.error("todo")
  }
  
  trait TableCompanion extends ColumnarTableCompanion {
    def apply(slices: StreamT[M, Slice]) = new Table(slices)

    def align(sourceLeft: Table, alignOnL: TransSpec1, sourceRight: Table, alignOnR: TransSpec1): M[(Table, Table)] = 
      sys.error("not implemented here")
  }

  object Table extends TableCompanion

  def newMemoContext = new MemoContext

  "a table dataset" should {
    "verify bijection from static JSON" in {
      val sample: List[JValue] = List(
        JObject(
          JField("key", JArray(JNum(-1L) :: JNum(0L) :: Nil)) ::
          JField("value", JNull) :: Nil
        ), 
        JObject(
          JField("key", JArray(JNum(-3090012080927607325l) :: JNum(2875286661755661474l) :: Nil)) ::
          JField("value", JObject(List(
            JField("q8b", JArray(List(
              JNum(6.615224799778253E307d), 
              JArray(List(JBool(false), JNull, JNum(-8.988465674311579E307d))), JNum(-3.536399224770604E307d)))), 
            JField("lwu",JNum(-5.121099465699862E307d))))
          ) :: Nil
        ), 
        JObject(
          JField("key", JArray(JNum(-3918416808128018609l) :: JNum(-1L) :: Nil)) ::
          JField("value", JNum(-1.0)) :: Nil
        )
      )

      val dataset = fromJson(sample.toStream)
      val results = dataset.toJson
      results.copoint must containAllOf(sample).only 
    }

    "verify bijection from JSON" in checkMappings(this)

    "in cogroup" >> {
      "perform a simple cogroup" in testSimpleCogroup
      "perform another simple cogroup" in testAnotherSimpleCogroup
      "perform yet another simple cogroup" in testAnotherSimpleCogroupSwitched
      "cogroup across slice boundaries" in testCogroupSliceBoundaries
      "error on unsorted inputs" in testUnsortedInputs

      "survive pathology 1" in testCogroupPathology1
      "survive pathology 2" in testCogroupPathology2
      "survive pathology 3" in testCogroupPathology3
      
      "survive scalacheck" in { 
        check { cogroupData: (SampleData, SampleData) => testCogroup(cogroupData._1, cogroupData._2) } 
      }
    }

    "in cross" >> {
      "perform a simple cartesian" in testSimpleCross
      "cross across slice boundaries on one side" in testCrossSingles
      "survive scalacheck" in {
        check { cogroupData: (SampleData, SampleData) => testCross(cogroupData._1, cogroupData._2) } 
      }
    }

    "in transform" >> {
      "perform the identity transform" in checkTransformLeaf
      "perform a trivial map1" in testMap1IntLeaf
      //"give the identity transform for the trivial filter" in checkTrivialFilter  //why is this commented out?
      "give the identity transform for the trivial 'true' filter" in checkTrueFilter
      "give the identity transform for a nontrivial filter" in checkFilter
      "perform an object dereference" in checkObjectDeref
      "perform an array dereference" in checkArrayDeref
      "perform a trivial map2" in checkMap2
      "perform a trivial equality check" in checkEqualSelf
      "perform a trivial equality check on an array" in checkEqualSelfArray
      "perform a slightly less trivial equality check" in checkEqual
      "wrap the results of a transform in an object as the specified field" in checkWrapObject
      "give the identity transform for self-object concatenation" in checkObjectConcatSelf
      "use a right-biased overwrite strategy in object concat conflicts" in checkObjectConcatOverwrite
      "concatenate dissimilar objects" in checkObjectConcat
      "concatenate dissimilar arrays" in checkArrayConcat
      "delete elements according to a JType" in checkObjectDelete
      "perform a trivial type-based filter" in checkTypedTrivial
      "perform a trivial heterogeneous type-based filter" in checkTypedHeterogeneous
      "perform a trivial object type-based filter" in checkTypedObject
      "perform another trivial object type-based filter" in checkTypedObject2
      "perform a trivial array type-based filter" in checkTypedArray
      "perform another trivial array type-based filter" in checkTypedArray2
      "perform yet another trivial array type-based filter" in checkTypedArray3
      "perform a fourth trivial array type-based filter" in checkTypedArray4
      "perform a trivial number type-based filter" in checkTypedNumber
      "perform another trivial number type-based filter" in checkTypedNumber2
      "perform a filter returning the empty set" in checkTypedEmpty
      "perform a less trivial type-based filter" in checkTyped
      "perform a summation scan case 1" in testTrivialScan
      "perform a summation scan of heterogeneous data" in testHetScan
      "perform a summation scan" in checkScan
      "perform dynamic object deref" in testDerefObjectDynamic
      "perform an array swap" in checkArraySwap
      "replace defined rows with a constant" in checkConst
    }

    "in compact" >> {
      "be the identity on fully defined tables"  in testCompactIdentity
      "preserve all defined rows"                in testCompactPreserve
      "have no undefined rows"                   in testCompactRows
      "have no empty slices"                     in testCompactSlices
      "preserve all defined key rows"            in testCompactPreserveKey
      "have no undefined key rows"               in testCompactRowsKey
      "have no empty key slices"                 in testCompactSlicesKey
    }
    
    "in distinct" >> {
      "be the identity on tables with no duplicate rows" in testDistinctIdentity
      "peform properly when the same row appears in two different slices" in testDistinctAcrossSlices
      "peform properly again when the same row appears in two different slices" in testDistinctAcrossSlices2
      "have no duplicate rows" in testDistinct
    }
  }

  "partitionMerge" should {
    "concatenate reductions of subsequences" in testPartitionMerge
  }

  "unionAll" should {
    "union a simple homogeneous borg result set" in simpleUnionAllTest
    "union a simple reversed borg result set" in reversedUnionAllTest
  }

  "crossAll" should {
    "cross a simple borg result set" in simpleCrossAllTest
  }
}

object ColumnarTableModuleSpec extends ColumnarTableModuleSpec[Free.Trampoline] {
  implicit def M = Trampoline.trampolineMonad

  type YggConfig = IdSourceConfig
  val yggConfig = new IdSourceConfig {
    val idSource = new IdSource {
      private val source = new java.util.concurrent.atomic.AtomicLong
      def nextId() = source.getAndIncrement
    }
  }
}


// vim: set ts=4 sw=4 et:
