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

import scalaz._
import scalaz.effect.IO 
import scalaz.syntax.copointed._

import org.specs2._
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

trait ColumnarTableModuleSpec[M[+_]] extends
  TableModuleSpec[M] with
  CogroupSpec[M] with
  CrossSpec[M] with
  TestColumnarTableModule[M] with
  TransformSpec[M] with
  BlockLoadSpec[M] with
  BlockSortSpec[M] with
  CompactSpec[M] with 
  DistinctSpec[M] { spec =>
    
  override val defaultPrettyParams = Pretty.Params(2)

  val testPath = Path("/tableOpsSpec")
  val actorSystem = ActorSystem("columnar-table-specs")
  implicit val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

  def lookupF1(namespace: List[String], name: String): F1 = {
    val lib = Map[String, CF1](
      "negate" -> cf.math.Negate,
      "true" -> new CF1P({ case _ => Column.const(true) })
    )

    lib(name)
  }

  def lookupF2(namespace: List[String], name: String): F2 = {
    val lib  = Map[String, CF2](
      "add" -> cf.math.Add,
      "mod" -> cf.math.Mod,
      "eq"  -> cf.std.Eq
    )
    lib(name)
  }

  def lookupScanner(namespace: List[String], name: String): CScanner = {
    val lib = Map[String, CScanner](
      "sum" -> new CScanner {
        type A = BigDecimal
        val init = BigDecimal(0)
        def scan(a: BigDecimal, cols: Set[Column], range: Range): (A, Set[Column]) = {
          val prioritized = cols.toSeq filter {
            case _: LongColumn | _: DoubleColumn | _: NumColumn => true
            case _ => false
          }
          
          val mask = BitSet(range filter { i => prioritized exists { _ isDefinedAt i } }: _*)
          
          val (a2, arr) = range.foldLeft((a, new Array[BigDecimal](range.end))) {
            case ((acc, arr), i) => {
              val col = prioritized find { _ isDefinedAt i }
              
              val acc2 = col map {
                case lc: LongColumn =>
                  acc + lc(i)
                
                case dc: DoubleColumn =>
                  acc + dc(i)
                
                case nc: NumColumn =>
                  acc + nc(i)
              }
              
              acc2 foreach { arr(i) = _ }
              
              (acc2 getOrElse acc, arr)
            }
          }
          
          (a2, Set(ArrayNumColumn(mask, arr)))
        }
      }
    )

    lib(name)
  }

  type Table = UnloadableTable
  class UnloadableTable(slices: StreamT[M, Slice]) extends ColumnarTable(slices) {
    import trans._
    def load(uid: UserId, jtpe: JType): M[Table] = sys.error("todo")
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder) = sys.error("todo")
  }
  
  type MemoContext = DummyMemoizationContext
  def newMemoContext = new DummyMemoizationContext

  def table(slices: StreamT[M, Slice]) = new UnloadableTable(slices)

  "a table dataset" should {
    //"verify bijection from static JSON" in {
    //  val sample: List[JValue] = List(
    //    JObject(
    //      JField("key", JArray(JNum(-1L) :: JNum(0L) :: Nil)) ::
    //      JField("value", JNull) :: Nil
    //    ), 
    //    JObject(
    //      JField("key", JArray(JNum(-3090012080927607325l) :: JNum(2875286661755661474l) :: Nil)) ::
    //      JField("value", JObject(List(
    //        JField("q8b", JArray(List(
    //          JNum(6.615224799778253E307d), 
    //          JArray(List(JBool(false), JNull, JNum(-8.988465674311579E307d))), JNum(-3.536399224770604E307d)))), 
    //        JField("lwu",JNum(-5.121099465699862E307d))))
    //      ) :: Nil
    //    ), 
    //    JObject(
    //      JField("key", JArray(JNum(-3918416808128018609l) :: JNum(-1L) :: Nil)) ::
    //      JField("value", JNum(-1.0)) :: Nil
    //    )
    //  )

    //  val dataset = fromJson(sample.toStream)
    //  //dataset.slices.foreach(println)
    //  val results = dataset.toJson
    //  results.copoint must containAllOf(sample).only 
    //}

    //"verify bijection from JSON" in checkMappings

    //"in cogroup" >> {
    //  "perform a simple cogroup" in testSimpleCogroup
    //  "perform another simple cogroup" in testAnotherSimpleCogroup
    //  "perform yet another simple cogroup" in testAnotherSimpleCogroupSwitched
    //  "cogroup across slice boundaries" in testCogroupSliceBoundaries
    //  "error on unsorted inputs" in testUnsortedInputs

    //  "survive pathology 1" in testCogroupPathology1
    //  "survive pathology 2" in testCogroupPathology2
    //  "survive pathology 3" in testCogroupPathology3
    //  
    //  "survive scalacheck" in { 
    //    check { cogroupData: (SampleData, SampleData) => testCogroup(cogroupData._1, cogroupData._2) } 
    //  }
    //}

    //"in cross" >> {
    //  "perform a simple cartesian" in testSimpleCross
    //  "cross across slice boundaries on one side" in testCrossSingles
    //  "survive scalacheck" in { 
    //    check { cogroupData: (SampleData, SampleData) => testCross(cogroupData._1, cogroupData._2) } 
    //  }
    //}

    "in transform" >> {
      //"perform the identity transform" in checkTransformLeaf
      //"perform a trivial map1" in testMap1IntLeaf
      ////"give the identity transform for the trivial filter" in checkTrivialFilter
      //"give the identity transform for the trivial 'true' filter" in checkTrueFilter
      //"give the identity transform for a nontrivial filter" in checkFilter
      //"perform an object dereference" in checkObjectDeref
      //"perform an array dereference" in checkArrayDeref
      //"perform a trivial map2" in checkMap2
      //"perform a trivial equality check" in checkEqualSelf
      //"perform a slightly less trivial equality check" in checkEqual
      //"wrap the results of a transform in an object as the specified field" in checkWrapObject
      //"give the identity transform for self-object concatenation" in checkObjectConcatSelf
      //"use a right-biased overwrite strategy in object concat conflicts" in checkObjectConcatOverwrite
      //"concatenate dissimilar objects" in checkObjectConcat
      //"concatenate dissimilar arrays" in checkArrayConcat
      //"delete elements according to a JType" in checkObjectDelete
      //"perform a trivial type-based filter" in checkTypedTrivial
      //"perform a trivial heterogeneous type-based filter" in checkTypedHeterogeneous
      //"perform a trivial object type-based filter" in checkTypedObject
      "perform another trivial object type-based filter" in checkTypedObject2
      //"perform a trivial array type-based filter" in checkTypedArray
      "perform another trivial array type-based filter" in checkTypedArray2
      //"perform a trivial number type-based filter" in checkTypedNumber
      //"perform another trivial number type-based filter" in checkTypedNumber2
      //"perform a filter returning the empty set" in checkTypedEmpty
      //"perform a less trivial type-based filter" in checkTyped
      //"perform a summation scan case 1" in testTrivialScan
      //"perform a summation scan" in checkScan
      //"perform dynamic object deref" in testDerefObjectDynamic
      //"perform an array swap" in checkArraySwap
      //"replace defined rows with a constant" in checkConst
    }

    //"in load" >> {
    //  "reconstruct a problem sample" in testLoadSample1
    //  "reconstruct a problem sample" in testLoadSample2
    //  "reconstruct a problem sample" in testLoadSample3
    //  "reconstruct a problem sample" in testLoadSample4
    //  //"reconstruct a problem sample" in testLoadSample5 //pathological sample in the case of duplicated ids.
    //  "reconstruct a dense dataset" in checkLoadDense
    //}                           

    //"sort" >> {
    //  "fully homogeneous data"        in homogeneousSortSample
    //  "data with undefined sort keys" in partiallyUndefinedSortSample
    //  "heterogeneous sort keys"       in heterogeneousSortSample
    //  "arbitrary datasets"            in checkSortDense
    //}
    //
    //"in compact" >> {
    //  "be the identity on fully defined tables"  in testCompactIdentity
    //  "preserve all defined rows"                in testCompactPreserve
    //  "have no undefined rows"                   in testCompactRows
    //  "have no empty slices"                     in testCompactSlices
    //  "preserve all defined key rows"            in testCompactPreserveKey
    //  "have no undefined key rows"               in testCompactRowsKey
    //  "have no empty key slices"                 in testCompactSlicesKey
    //}
    //
    //"in distinct" >> {
    //  "be the identity on tables with no duplicate rows" in testDistinctIdentity
    //  "have no duplicate rows" in testDistinct
    //}
  }
}

object ColumnarTableModuleSpec extends ColumnarTableModuleSpec[Free.Trampoline] {
  implicit def M = Trampoline.trampolineMonad
  implicit def coM = Trampoline.trampolineMonad

  type YggConfig = IdSourceConfig
  val yggConfig = new IdSourceConfig {
    val idSource = new IdSource {
      private val source = new java.util.concurrent.atomic.AtomicLong
      def nextId() = source.getAndIncrement
    }
  }
}


// vim: set ts=4 sw=4 et:
