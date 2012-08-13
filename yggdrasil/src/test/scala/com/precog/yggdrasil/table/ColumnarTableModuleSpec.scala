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

import akka.actor.ActorSystem
import akka.dispatch._
import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._

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

trait ColumnarTableModuleSpec[M[+_]] extends TableModuleSpec[M] with TestColumnarTableModule[M] with TransformSpec[M] with BlockLoadSpec[M] with CogroupSpec[M] with BlockSortSpec[M] { spec =>
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
        def scan(a: BigDecimal, col: Column, range: Range): (A, Option[Column]) = {
          col match {
            case lc: LongColumn => 
              val (a0, acc) = range.foldLeft((a, new Array[BigDecimal](range.end))) {
                case ((a0, acc), i) => 
                  val intermediate = a0 + lc(i)
                  acc(i) = intermediate
                  (intermediate, acc)
              }

              (a0, Some(ArrayNumColumn(BitSet(range: _*), acc)))
              
            case lc: DoubleColumn =>
              val (a0, acc) = range.foldLeft((a, new Array[BigDecimal](range.end))) {
                case ((a0, acc), i) => 
                  val intermediate = a0 + lc(i)
                  acc(i) = intermediate
                  (intermediate, acc)
              }

              (a0, Some(ArrayNumColumn(BitSet(range: _*), acc)))
              
            case lc: NumColumn =>
              val (a0, acc) = range.foldLeft((a, new Array[BigDecimal](range.end))) {
                case ((a0, acc), i) => 
                  val intermediate = a0 + lc(i)
                  acc(i) = intermediate
                  (intermediate, acc)
              }

              (a0, Some(ArrayNumColumn(BitSet(range: _*), acc)))

            case _ => (a, None)
          }
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

  def table(slices: StreamT[M, Slice]) = new UnloadableTable(slices)

  "a table dataset" should {
//    "verify bijection from static JSON" in {
//      val sample: List[JValue] = List(
//        JObject(
//          JField("key", JArray(JNum(-1L) :: JNum(0L) :: Nil)) ::
//          JField("value", JNull) :: Nil
//        ), 
//        JObject(
//          JField("key", JArray(JNum(-3090012080927607325l) :: JNum(2875286661755661474l) :: Nil)) ::
//          JField("value", JObject(List(
//            JField("q8b", JArray(List(
//              JNum(6.615224799778253E307d), 
//              JArray(List(JBool(false), JNull, JNum(-8.988465674311579E307d))), JNum(-3.536399224770604E307d)))), 
//            JField("lwu",JNum(-5.121099465699862E307d))))
//          ) :: Nil
//        ), 
//        JObject(
//          JField("key", JArray(JNum(-3918416808128018609l) :: JNum(-1L) :: Nil)) ::
//          JField("value", JNum(-1.0)) :: Nil
//        )
//      )
//
//      val dataset = fromJson(sample.toStream)
//      //dataset.slices.foreach(println)
//      val results = dataset.toJson
//      results.copoint must containAllOf(sample).only 
//    }
//
//    "verify bijection from JSON" in checkMappings
//
//    "in cogroup" >> {
//      "perform a simple cogroup" in testSimpleCogroup
//      "cogroup across slice boundaries" in testCogroupSliceBoundaries
//
//      "survive pathology 1" in testCogroupPathology1
//      "survive pathology 2" in testCogroupPathology2
//
//      "survive scalacheck" in { 
//        check { cogroupData: (SampleData, SampleData) => testCogroup(cogroupData._1, cogroupData._2) } 
//      }
//    }
//
//    "in transform" >> {
//      "perform the identity transform" in checkTransformLeaf
//      "perform a trivial map1" in testMap1IntLeaf
//      //"give the identity transform for the trivial filter" in checkTrivialFilter
//      "give the identity transform for the trivial 'true' filter" in checkTrueFilter
//      "give the identity transform for a nontrivial filter" in checkFilter
//      "perform an object dereference" in checkObjectDeref
//      "perform an array dereference" in checkArrayDeref
//      "perform a trivial map2" in checkMap2
//      "perform a trivial equality check" in checkEqualSelf
//      "perform a slightly less trivial equality check" in checkEqual
//      "wrap the results of a transform in an object as the specified field" in checkWrapObject
//      "give the identity transform for self-object concatenation" in checkObjectConcatSelf
//      "use a right-biased overwrite strategy in object concat conflicts" in checkObjectConcatOverwrite
//      "concatenate dissimilar objects" in checkObjectConcat
//      "concatenate dissimilar arrays" in checkArrayConcat
//      "delete elements according to a JType" in checkObjectDelete
//      "perform a trivial type-based filter" in checkTypedTrivial
//      "perform a less trivial type-based filter" in checkTyped
//      "perform a summation scan" in checkScan
//      "perform dynamic object deref" in testDerefObjectDynamic
//      "perform an array swap" in checkArraySwap
//    }
//
//    "in load" >> {
//      "reconstruct a problem sample" in testLoadSample1
//      "reconstruct a problem sample" in testLoadSample2
//      "reconstruct a problem sample" in testLoadSample3
//      "reconstruct a problem sample" in testLoadSample4
//      //"reconstruct a problem sample" in testLoadSample5 //pathological sample in the case of duplicated ids.
//      "reconstruct a dense dataset" in checkLoadDense
//    }
//
    "sort" >> {
//      "fully homogeneous data"        in homogeneousSortSample
//      "data with undefined sort keys" in partiallyUndefinedSortSample
//      "heterogeneous sort keys"       in heterogeneousSortSample
      "arbitrary datasets"            in checkSortDense
    }    
  }
}

object ColumnarTableModuleSpec extends ColumnarTableModuleSpec[Free.Trampoline] {
  implicit def M = Trampoline.trampolineMonad
  implicit def coM = Trampoline.trampolineMonad
}


// vim: set ts=4 sw=4 et:
