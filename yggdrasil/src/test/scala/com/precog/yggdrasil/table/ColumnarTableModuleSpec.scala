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

trait ColumnarTableModuleSpec[M[+_]] extends TableModuleSpec[M] with CogroupSpec[M] with TestColumnarTableModule[M] with TransformSpec[M] with BlockLoadSpec[M] with BlockSortSpec[M] { spec =>
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
    "verify bijection from static JSON" in {
      val sample: List[JValue] = List(
        JObject(
          JField("key", JArray(JInt(-1L) :: JInt(0L) :: Nil)) ::
          JField("value", JNull) :: Nil
        ), 
        JObject(
          JField("key", JArray(JInt(-3090012080927607325l) :: JInt(2875286661755661474l) :: Nil)) ::
          JField("value", JObject(List(
            JField("q8b", JArray(List(
              JDouble(6.615224799778253E307d), 
              JArray(List(JBool(false), JNull, JDouble(-8.988465674311579E307d))), JDouble(-3.536399224770604E307d)))), 
            JField("lwu",JDouble(-5.121099465699862E307d))))
          ) :: Nil
        ), 
        JObject(
          JField("key", JArray(JInt(-3918416808128018609l) :: JInt(-1L) :: Nil)) ::
          JField("value", JDouble(-1.0)) :: Nil
        )
      )

      val dataset = fromJson(sample.toStream)
      //dataset.slices.foreach(println)
      val results = dataset.toJson
      results.copoint must containAllOf(sample).only 
    }

    "verify bijection from JSON" in checkMappings

    "in cogroup" >> {
      //"survive scalacheck" in { 
      //  check { cogroupData: (SampleData, SampleData) => testCogroup(cogroupData._1, cogroupData._2) } 
      //}

      "cogroup across slice boundaries" in testCogroupSliceBoundaries
      "survive pathology 1" in testCogroupPathology1
      "survive pathology 2" in testCogroupPathology2
    }

    "in transform" >> {
      "perform the identity transform" in checkTransformLeaf
      "perform a trivial map1" in testMap1IntLeaf
      //"give the identity transform for the trivial filter" in checkTrivialFilter
      "give the identity transform for the trivial 'true' filter" in checkTrueFilter
      "give the identity transform for a nontrivial filter" in checkFilter
      "perform an object dereference" in checkObjectDeref
      "perform an array dereference" in checkArrayDeref
      "perform a trivial map2" in checkMap2
      "perform a trivial equality check" in checkEqualSelf
      "perform a slightly less trivial equality check" in checkEqual
      "wrap the results of a transform in an object as the specified field" in checkWrapObject
      "give the identity transform for self-object concatenation" in checkObjectConcatSelf
      "use a right-biased overwrite strategy in object concat conflicts" in checkObjectConcatOverwrite
      "concatenate dissimilar objects" in checkObjectConcat
      "concatenate dissimilar arrays" in checkArrayConcat
      "delete elements according to a JType" in checkObjectDelete
      "perform a trivial type-based filter" in checkTypedTrivial
      "perform a less trivial type-based filter" in checkTyped
      "perform a summation scan" in checkScan
      "perform dynamic object deref" in testDerefObjectDynamic
      "perform an array swap" in checkArraySwap
    }

    "in load" >> {
      "reconstruct a problem sample" in testLoadSample1
      "reconstruct a problem sample" in testLoadSample2
      "reconstruct a problem sample" in testLoadSample3
      "reconstruct a problem sample" in testLoadSample4
      //"reconstruct a problem sample" in testLoadSample5 //pathological sample in the case of duplicated ids.
      "reconstruct a dense dataset" in checkLoadDense
    }

    "sort" >> {
      "fully homogeneous data"        in homogeneousSortSample
      "data with undefined sort keys" in partiallyUndefinedSortSample
      "heterogeneous sort keys"       in heterogeneousSortSample
      "arbitrary datasets"            in checkSortDense
    }    
  }
}

object ColumnarTableModuleSpec extends ColumnarTableModuleSpec[Free.Trampoline] {
  implicit def M = Trampoline.trampolineMonad
  implicit def coM = Trampoline.trampolineMonad
}


// vim: set ts=4 sw=4 et:
