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

import com.precog.common.{ Path, VectorCase } 
import com.precog.common.json._
import com.precog.common.security._
import com.precog.bytecode.JType
import com.precog.yggdrasil.util._

import akka.actor.ActorSystem
import akka.dispatch._
import blueeyes.json._
import org.slf4j.{LoggerFactory, MDC}

import scala.annotation.tailrec
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
import SampleData._

trait TestColumnarTableModule[M[+_]] extends ColumnarTableModuleTestSupport[M] 
    with TableModuleSpec[M]
    with CogroupSpec[M]
    with CrossSpec[M]
    with TransformSpec[M]
    with CompactSpec[M] 
    with TakeRangeSpec[M]
    with ToArraySpec[M]
    with ConcatSpec[M]
    with SampleSpec[M]
    with PartitionMergeSpec[M]
    with DistinctSpec[M] 
    with SchemasSpec[M]
    { spec => 

  type GroupId = Int
  import trans._
  import constants._
 
  private val groupId = new java.util.concurrent.atomic.AtomicInteger
  def newGroupId = groupId.getAndIncrement

  class Table(slices: StreamT[M, Slice], size: TableSize) extends ColumnarTable(slices, size) {
    import trans._
    def load(apiKey: APIKey, jtpe: JType): M[Table] = sys.error("todo")
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean = false) = M.point(this)
    def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder = SortAscending, unique: Boolean = false): M[Seq[Table]] = sys.error("todo")
  }
  
  trait TableCompanion extends ColumnarTableCompanion {
    def apply(slices: StreamT[M, Slice], size: TableSize) = new Table(slices, size)

    def singleton(slice: Slice) = new Table(slice :: StreamT.empty[M, Slice], ExactSize(1))

    def align(sourceLeft: Table, alignOnL: TransSpec1, sourceRight: Table, alignOnR: TransSpec1): M[(Table, Table)] = 
      sys.error("not implemented here")
  }

  object Table extends TableCompanion
}

trait ColumnarTableModuleSpec[M[+_]] extends TestColumnarTableModule[M] 
    with TableModuleSpec[M]
    with CogroupSpec[M]
    with CrossSpec[M]
    with TransformSpec[M]
    with CompactSpec[M] 
    with TakeRangeSpec[M]
    with PartitionMergeSpec[M]
    with DistinctSpec[M] 
    with SchemasSpec[M]
    { spec => 

  import trans._
  import constants._
    
  override val defaultPrettyParams = Pretty.Params(2)

  lazy val xlogger = LoggerFactory.getLogger("com.precog.yggdrasil.table.ColumnarTableModuleSpec")

  "a table dataset" should {
    "verify bijection from static JSON" in {
      val sample: List[JValue] = List(
        JObject(
          JField("key", JArray(JNum(-1L), JNum(0L))),
          JField("value", JNull)
        ), 
        JObject(
          JField("key", JArray(JNum(-3090012080927607325l), JNum(2875286661755661474l))),
          JField("value", JObject(
            JField("q8b", JArray(
              JNum(6.615224799778253E307d), 
              JArray(JBool(false), JNull, JNum(-8.988465674311579E307d), JNum(-3.536399224770604E307d))
            )), 
            JField("lwu",JNum(-5.121099465699862E307d))
          ))
        ), 
        JObject(
          JField("key", JArray(JNum(-3918416808128018609l), JNum(-1L))),
          JField("value", JNum(-1.0))
        )
      )

      val dataset = fromJson(sample.toStream)
      val results = dataset.toJson
      results.copoint must containAllOf(sample).only
    }

    "verify bijection from JSON" in checkMappings(this)
    
    "verify renderJson round tripping" in {
      implicit val gen = sample(schema)
      
      check { data: SampleData =>
        testRenderJson(data.data)
      }.set(minTestsOk -> 20000, workers -> Runtime.getRuntime.availableProcessors)
    }
    
    "handle special cases of renderJson" >> {
      "undefined at beginning of array" >> {
        testRenderJson(JArray(
          JUndefined ::
          JNum(1) ::
          JNum(2) :: Nil) :: Nil)
      }
      
      "undefined in middle of array" >> {
        testRenderJson(JArray(
          JNum(1) ::
          JUndefined ::
          JNum(2) :: Nil) :: Nil)
      }
      
      "fully undefined array" >> {
        testRenderJson(JArray(
          JUndefined ::
          JUndefined ::
          JUndefined :: Nil) :: Nil)
      }
      
      "undefined at beginning of object" >> {
        testRenderJson(JObject(
          JField("foo", JUndefined) ::
          JField("bar", JNum(1)) ::
          JField("baz", JNum(2)) :: Nil) :: Nil)
      }
      
      "undefined in middle of object" >> {
        testRenderJson(JObject(
          JField("foo", JNum(1)) ::
          JField("bar", JUndefined) ::
          JField("baz", JNum(2)) :: Nil) :: Nil)
      }
      
      "fully undefined object" >> {
        //testRenderJson(JObject(
        //  JField("foo", JUndefined) ::
        //  JField("bar", JUndefined) ::
        //  JField("baz", JUndefined) :: Nil) :: Nil)
        testRenderJson(
          JObject(
            Map(
              "foo" -> JUndefined,
              "bar" -> JUndefined,
              "baz" -> JUndefined
            )
          ) :: Nil
        )
      }
      
      "undefined row" >> {
        testRenderJson(
          JObject(
            JField("foo", JUndefined) ::
            JField("bar", JUndefined) ::
            JField("baz", JUndefined) :: Nil) ::
          JNum(42) :: Nil)
      }
      
      "check utf-8 encoding" in check { str: String =>
        val s = str.toList.map((c: Char) => if (c < ' ') ' ' else c).mkString
        testRenderJson(JString(s) :: Nil)
      }.set(minTestsOk -> 20000, workers -> Runtime.getRuntime.availableProcessors)
      
      "check long encoding" in check { ln: Long =>
        testRenderJson(JNum(ln) :: Nil)
      }.set(minTestsOk -> 20000, workers -> Runtime.getRuntime.availableProcessors)
    }
    
    def testRenderJson(seq: Seq[JValue]) = {
      def arr(es: List[JValue]) = if (es.isEmpty) None else Some(JArray(es))

      def minimizeItem(t: (String, JValue)) = minimize(t._2).map((t._1, _))

      def minimize(value: JValue): Option[JValue] = {
        value match {
          case JObject(fields) => Some(JObject(fields.flatMap(minimizeItem)))

          case JArray(Nil) => Some(JArray(Nil))

          case JArray(elements) =>
            val elements2 = elements.flatMap(minimize)
            if (elements2.isEmpty) None else Some(JArray(elements2))

          case JUndefined => None

          case v => Some(v)
        }
      }
    
      val table = fromJson(seq.toStream)
      
      val expected = JArray(seq.toList)
      
      val values = table.renderJson(',') map { _.toString }
      
      val strM = values.foldLeft("") { _ + _ }
      
      val arrayM = strM map { body =>
        val input = "[%s]".format(body)
        JParser.parse(input)
      }
      
      val minimized = minimize(expected) getOrElse JArray(Nil)
      arrayM.copoint mustEqual minimized
    }
    
    "in cogroup" >> {
      "perform a trivial cogroup" in testTrivialCogroup(identity[Table])
      "perform a simple cogroup" in testSimpleCogroup(identity[Table])
      "perform another simple cogroup" in testAnotherSimpleCogroup
      "cogroup for unions" in testUnionCogroup
      "perform yet another simple cogroup" in testAnotherSimpleCogroupSwitched
      "cogroup across slice boundaries" in testCogroupSliceBoundaries
      "error on unsorted inputs" in testUnsortedInputs
      "cogroup partially defined inputs properly" in testPartialUndefinedCogroup

      "survive pathology 1" in testCogroupPathology1
      "survive pathology 2" in testCogroupPathology2
      "survive pathology 3" in testCogroupPathology3
      
      "survive scalacheck" in { 
        check { cogroupData: (SampleData, SampleData) => testCogroup(cogroupData._1, cogroupData._2) } 
      }
    }

    "in cross" >> {
      "perform a simple cartesian" in testSimpleCross
      
      "split a cross that would exceed slice boundaries" in {
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
        
        val dataset1 = fromJson(sample.toStream, Some(3))
        val dataset2 = fromJson(sample.toStream, Some(3))
        
        dataset1.cross(dataset1)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight))).slices.uncons.copoint must beLike {
          case Some((head, _)) => head.size must beLessThanOrEqualTo(3)
        }
      }
      
      "cross across slice boundaries on one side" in testCrossSingles
      "survive scalacheck" in {
        check { cogroupData: (SampleData, SampleData) => testCross(cogroupData._1, cogroupData._2) } 
      }
    }

    "in transform" >> {
      "perform the identity transform" in checkTransformLeaf
      "perform a trivial map1" in testMap1IntLeaf
      "perform deepmap1 using numeric coercion" in testDeepMap1CoerceToDouble
      "perform map1 using numeric coercion" in testMap1CoerceToDouble
      "fail to map1 into array and object" in testMap1ArrayObject
      "perform a less trvial map1" in checkMap1
      //"give the identity transform for the trivial filter" in checkTrivialFilter
      "give the identity transform for the trivial 'true' filter" in checkTrueFilter
      "give the identity transform for a nontrivial filter" in checkFilter
      "give a transformation for a big decimal and a long" in testMod2Filter
      "perform an object dereference" in checkObjectDeref
      "perform an array dereference" in checkArrayDeref
      "perform metadata dereference on data without metadata" in checkMetaDeref
      "perform a trivial map2 add" in checkMap2Add
      "perform a trivial map2 eq" in checkMap2Eq
      "perform a map2 add over but not into arrays and objects" in testMap2ArrayObject
      "perform a trivial equality check" in checkEqualSelf
      "perform a trivial equality check on an array" in checkEqualSelfArray
      "perform a slightly less trivial equality check" in checkEqual
      "test a failing equality example" in testEqual1
      "perform a simple equality check" in testSimpleEqual
      "perform another simple equality check" in testAnotherSimpleEqual
      "perform yet another simple equality check" in testYetAnotherSimpleEqual
      "perform a simple not-equal check" in testASimpleNonEqual
      "perform a equal-literal check" in checkEqualLiteral
      "perform a not-equal-literal check" in checkNotEqualLiteral
      "wrap the results of a transform in an object as the specified field" in checkWrapObject
      "give the identity transform for self-object concatenation" in checkObjectConcatSelf
      "use a right-biased overwrite strategy in object concat conflicts" in checkObjectConcatOverwrite
      "test inner object concat with a single boolean" in testObjectConcatSingletonNonObject
      "test inner object concat with a boolean and an empty object" in testObjectConcatTrivial
      "concatenate dissimilar objects" in checkObjectConcat
      "test inner object concat join semantics" in testInnerObjectConcatJoinSemantics
      "test inner object concat with empty objects" in testInnerObjectConcatEmptyObject
      "test outer object concat with empty objects" in testOuterObjectConcatEmptyObject
      "test inner object concat with undefined" in testInnerObjectConcatUndefined
      "test outer object concat with undefined" in testOuterObjectConcatUndefined
      "test inner object concat with empty" in testInnerObjectConcatLeftEmpty
      "test outer object concat with empty" in testOuterObjectConcatLeftEmpty
      "concatenate dissimilar arrays" in checkArrayConcat
      "inner concatenate arrays with undefineds" in testInnerArrayConcatUndefined
      "outer concatenate arrays with undefineds" in testOuterArrayConcatUndefined
      "inner concatenate arrays with empty arrays" in testInnerArrayConcatEmptyArray
      "outer concatenate arrays with empty arrays" in testOuterArrayConcatEmptyArray
      "inner array concatenate when one side is not an array" in testInnerArrayConcatLeftEmpty
      "outer array concatenate when one side is not an array" in testOuterArrayConcatLeftEmpty

      "delete elements according to a JType" in checkObjectDelete
      "delete only field in object without removing from array" in {
        val JArray(elements) = JParser.parse("""[
          {"foo": 4, "bar": 12},
          {"foo": 5},
          {"bar": 45},
          {},
          {"foo": 7, "bar" :23, "baz": 24}
        ]""")

        val sample = SampleData(elements.toStream)
        val table = fromSample(sample)

        val spec = ObjectDelete(Leaf(Source), Set(CPathField("foo")))
        val results = toJson(table.transform(spec))
        val JArray(expected) = JParser.parse("""[
          {"bar": 12},
          {},
          {"bar": 45},
          {},
          {"bar" :23, "baz": 24}
        ]""")

        results.copoint mustEqual expected.toStream
      }

      "perform a trivial type-based filter" in checkTypedTrivial
      "perform a less trivial type-based filter" in checkTyped
      "perform a type-based filter across slice boundaries" in testTypedAtSliceBoundary
      "perform a trivial heterogeneous type-based filter" in testTypedHeterogeneous
      "perform a trivial object type-based filter" in testTypedObject
      "retain all object members when typed to unfixed object" in testTypedObjectUnfixed
      "perform another trivial object type-based filter" in testTypedObject2
      "perform a trivial array type-based filter" in testTypedArray
      "perform another trivial array type-based filter" in testTypedArray2
      "perform yet another trivial array type-based filter" in testTypedArray3
      "perform a fourth trivial array type-based filter" in testTypedArray4
      "perform a trivial number type-based filter" in testTypedNumber
      "perform another trivial number type-based filter" in testTypedNumber2
      "perform a filter returning the empty set" in testTypedEmpty

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

    "in takeRange" >> {
      "select the correct rows in a trivial case" in testTakeRange
      "select the correct rows when we take past the end of the table" in testTakeRangeLarger
      "select the correct rows when we start at an index larger than the size of the table" in testTakeRangeEmpty
      "select the correct rows across slice boundary" in testTakeRangeAcrossSlices
      "select the correct rows only in second slice" in testTakeRangeSecondSlice
      "select the first slice" in testTakeRangeFirstSliceOnly
      "select nothing with a negative starting index" in testTakeRangeNegStart
      "select nothing with a negative number to take" in testTakeRangeNegNumber
      "select the correct rows using scalacheck" in checkTakeRange
    }

    "in toArray" >> {
      "create a single column given two single columns" in testToArrayHomogeneous
      "create a single column given heterogeneous data" in testToArrayHeterogeneous
    }

    "in concat" >> {
      "concat two tables" in testConcat
    }

    "in schemas" >> {
      "find a schema in single-schema table" in testSingleSchema
      "find a schema in homogeneous array table" in testHomogeneousArraySchema
      "find schemas separated by slice boundary" in testCrossSliceSchema
      "extract intervleaved schemas" in testIntervleavedSchema
      "don't include undefineds in schema" in testUndefinedsInSchema
      "deal with most expected types" in testAllTypesInSchema
    }

    "in sample" >> {
       "sample from a dataset" in testSample
       "return no samples given empty sequence of transspecs" in testSampleEmpty
       "sample from a dataset given non-identity transspecs" in testSampleTransSpecs
       "return full set when sample size larger than dataset" in testLargeSampleSize
       "resurn empty table when sample size is 0" in test0SampleSize
    }
  }

  "partitionMerge" should {
    "concatenate reductions of subsequences" in testPartitionMerge
  }

  "logging" should {
    "run" in {
      testSimpleCogroup(t => t.logged(xlogger, "test-logging", "start stream", "end stream") {
        slice => "size: " + slice.size
      })
    }
  }

  "track table metrics" in {
    "single traversal" >> {
      implicit val gen = sample(objectSchema(_, 3))
      check { (sample: SampleData) =>
        val expectedSlices = (sample.data.size.toDouble / defaultSliceSize).ceil

        val table = fromSample(sample)
        val t0 = table.transform(TransSpec1.Id)
        t0.toJson.copoint must_== sample.data

        table.metrics.startCount must_== 1
        table.metrics.sliceTraversedCount must_== expectedSlices
        t0.metrics.startCount must_== 1
        t0.metrics.sliceTraversedCount must_== expectedSlices
      }
    }

    "multiple transforms" >> {
      implicit val gen = sample(objectSchema(_, 3))
      check { (sample: SampleData) =>
        val expectedSlices = (sample.data.size.toDouble / defaultSliceSize).ceil

        val table = fromSample(sample)
        val t0 = table.transform(TransSpec1.Id).transform(TransSpec1.Id).transform(TransSpec1.Id)
        t0.toJson.copoint must_== sample.data

        table.metrics.startCount must_== 1
        table.metrics.sliceTraversedCount must_== expectedSlices
        t0.metrics.startCount must_== 1
        t0.metrics.sliceTraversedCount must_== expectedSlices
      }
    }

    "multiple forcing calls" >> {
      implicit val gen = sample(objectSchema(_, 3))
      check { (sample: SampleData) =>
        val expectedSlices = (sample.data.size.toDouble / defaultSliceSize).ceil

        val table = fromSample(sample)
        val t0 = table.compact(TransSpec1.Id).compact(TransSpec1.Id).compact(TransSpec1.Id)
        table.toJson.copoint must_== sample.data
        t0.toJson.copoint must_== sample.data

        table.metrics.startCount must_== 2
        table.metrics.sliceTraversedCount must_== (expectedSlices * 2)
        t0.metrics.startCount must_== 1
        t0.metrics.sliceTraversedCount must_== expectedSlices
      }
    }
  }
}

object ColumnarTableModuleSpec extends ColumnarTableModuleSpec[Free.Trampoline] {
  implicit def M = Trampoline.trampolineMonad

  type YggConfig = IdSourceConfig with ColumnarTableModuleConfig
  val yggConfig = new IdSourceConfig with ColumnarTableModuleConfig {
    val maxSliceSize = 10
    
    val idSource = new FreshAtomicIdSource
  }
}


// vim: set ts=4 sw=4 et:
