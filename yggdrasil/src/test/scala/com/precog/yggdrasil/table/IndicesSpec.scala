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
import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

import TableModule._
import SampleData._

// TODO: mix in a trait rather than defining Table directly

trait IndicesSpec[M[+_]] extends ColumnarTableModuleTestSupport[M]
    with TableModuleSpec[M]
    with IndicesModule[M] { spec =>

  type GroupId = Int
  import trans._
  import constants._

  import TableModule._
  import trans._
  import trans.constants._

  import Table._
  import SliceTransform._
  import TransSpec.deepMap

  private val groupId = new java.util.concurrent.atomic.AtomicInteger
  def newGroupId = groupId.getAndIncrement

  class Table(slices: StreamT[M, Slice], size: TableSize) extends ColumnarTable(slices, size) {
    import trans._
    def load(apiKey: APIKey, jtpe: JType): M[Table] = sys.error("todo")
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean = false) = sys.error("todo")
    def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder = SortAscending, unique: Boolean = false): M[Seq[Table]] = sys.error("todo")
  }
  
  trait TableCompanion extends ColumnarTableCompanion {
    def apply(slices: StreamT[M, Slice], size: TableSize) = new Table(slices, size)

    def singleton(slice: Slice) = new Table(slice :: StreamT.empty[M, Slice], ExactSize(1))

    def align(sourceLeft: Table, alignOnL: TransSpec1, sourceRight: Table, alignOnR: TransSpec1):
        M[(Table, Table)] = sys.error("not implemented here")
  }

  object Table extends TableCompanion

  def groupkey(s: String) = DerefObjectStatic(Leaf(Source), CPathField(s))
  def valuekey(s: String) = DerefObjectStatic(Leaf(Source), CPathField(s))

  "a table index" should {
    "handle empty tables" in {
      val table = fromJson(Stream.empty[JValue])

      val keySpecs = Array(groupkey("a"), groupkey("b"))
      val valSpec = valuekey("c")

      val index: TableIndex = TableIndex.createFromTable(table, keySpecs, valSpec).copoint

      index.getUniqueKeys(0).size must_== 0
      index.getSubTable(Array(0), Array(JString("a"))).size == ExactSize(0)
    }

    val json = """
{"a": 1, "b": 2, "c": 3}
{"a": 1, "b": 2, "c": 999, "d": "foo"}
{"a": 1, "b": 2, "c": "cat"}
{"a": 1, "b": 2}
{"a": 2, "b": 2, "c": 3, "d": 1248}
{"a": 2, "b": 2, "c": 13}
{"a": "foo", "b": "bar", "c": 3}
{"a": 3, "b": "", "c": 333}
{"a": 3, "b": 2, "c": [1,2,3,4]}
{"a": 1, "b": 2, "c": {"cat": 13, "dog": 12}}
{"a": "foo", "b": 999}
{"b": 2, "c": 9876}
{"a": 1, "c": [666]}
"""

    val vs: Seq[JValue] = JParser.parseManyFromString(json).valueOr(throw _)

    val table = fromJson(vs.toStream)

    val keySpecs = Array(groupkey("a"), groupkey("b"))
    val valSpec = valuekey("c")

    val index: TableIndex = TableIndex.createFromTable(table, keySpecs, valSpec).copoint

    "determine unique groupkey values" in {
      index.getUniqueKeys(0) must_== Set(JNum(1), JNum(2), JNum(3), JString("foo"))
      index.getUniqueKeys(1) must_== Set(JNum(2), JNum(999), JString("bar"), JString(""))
    }

    "determine unique groupkey sets" in {
      index.getUniqueKeys() must_== Set[Seq[JValue]](
        Array(JNum(1), JNum(2)),
        Array(JNum(2), JNum(2)),
        Array(JString("foo"), JString("bar")),
        Array(JNum(3), JString("")),
        Array(JNum(3), JNum(2)),
        Array(JString("foo"), JNum(999))
      )
    }

    def subtableSet(index: TableIndex, ids: Seq[Int], vs: Seq[JValue]): Set[JValue] =
      index.getSubTable(ids, vs).toJson.copoint.toSet

    def test(vs: Seq[JValue], result: Set[JValue]): Unit =
      subtableSet(index, Array(0, 1), vs) must_== result

    "generate subtables based on groupkeys" in {
      def empty = Set.empty[JValue]

      test(Array(JNum(1), JNum(1)), empty)

      test(Array(JNum(1), JNum(2)), s1)
      def s1 = Set[JValue](
        JNum(3),
        JNum(999),
        JString("cat"),
        JObject(Map("cat" -> JNum(13), "dog" -> JNum(12)))
      )

      test(Array(JNum(2), JNum(2)), s2)
      def s2 = Set[JValue](JNum(3), JNum(13))

      test(Array(JString("foo"), JString("bar")), s3)
      def s3 = Set[JValue](JNum(3))

      test(Array(JNum(3), JString("")), s4)
      def s4 = Set[JValue](JNum(333))

      test(Array(JNum(3), JNum(2)), s5)
      def s5 = Set[JValue](JArray(JNum(1), JNum(2), JNum(3), JNum(4)))

      test(Array(JString("foo"), JNum(999)), empty)
    }

    val index1 = TableIndex.createFromTable(
      table, Array(groupkey("a")), valuekey("c")
    ).copoint

    val index2 = TableIndex.createFromTable(
      table, Array(groupkey("b")), valuekey("c")
    ).copoint

    "efficiently combine to produce unions" in {

      def tryit(tpls: (TableIndex, Seq[Int], Seq[JValue])*)(expected: JValue*) {
        val table = TableIndex.joinSubTables(tpls.toList)
        table.toJson.copoint.toSet must_== expected.toSet
      }

      // both disjunctions have data
      tryit(
        (index1, Seq(0), Seq(JNum(1))),
        (index2, Seq(0), Seq(JNum(2)))
      )(
        JNum(3),
        JNum(999),
        JNum(9876),
        JString("cat"),
        JNum(13),
        JArray(JNum(1), JNum(2), JNum(3), JNum(4)),
        JArray(JNum(666)),
        JObject(Map("cat" -> JNum(13), "dog" -> JNum(12)))
      )

      // only first disjunction has data
      tryit(
        (index1, Seq(0), Seq(JNum(1))),
        (index2, Seq(0), Seq(JNum(1234567)))
      )(
        JNum(3),
        JNum(999),
        JString("cat"),
        JArray(JNum(666)),
        JObject(Map("cat" -> JNum(13), "dog" -> JNum(12)))
      )

      // only second disjunction has data
      tryit(
        (index1, Seq(0), Seq(JNum(-8000))),
        (index2, Seq(0), Seq(JNum(2)))
      )(
        JNum(3),
        JNum(999),
        JNum(9876),
        JString("cat"),
        JNum(13),
        JArray(JNum(1), JNum(2), JNum(3), JNum(4)),
        JObject(Map("cat" -> JNum(13), "dog" -> JNum(12)))
      )

      // neither disjunction has data
      tryit(
        (index1, Seq(0), Seq(JNum(-8000))),
        (index2, Seq(0), Seq(JNum(1234567)))
      )()
    }
  }
}

object IndicesSpec extends IndicesSpec[Free.Trampoline] {
  implicit def M = Trampoline.trampolineMonad

  type YggConfig = IdSourceConfig with ColumnarTableModuleConfig

  val yggConfig = new IdSourceConfig with ColumnarTableModuleConfig {
    val maxSliceSize = 10
    
    val idSource = new FreshAtomicIdSource
  }
}

// vim: set ts=4 sw=4 et:
