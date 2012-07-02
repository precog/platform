package com.precog.yggdrasil
package table

import com.precog.common.Path
import com.precog.common.VectorCase

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser

import scala.annotation.tailrec
import scala.collection.BitSet

import scalaz._

import org.specs2._
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

class ColumnarTableModuleSpec extends TableModuleSpec with CogroupSpec with ColumnarTableModule with TransformSpec { spec =>
  override val defaultPrettyParams = Pretty.Params(2)

  val sliceSize = 10
  val testPath = Path("/tableOpsSpec")

  def debugPrint(dataset: Table): Unit = {
    println("\n\n")
    for (slice <- dataset.slices; i <- 0 until slice.size) println(slice.toString(i))
  }

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

  def lookupScanner(namespace: List[String], name: String) = sys.error("todo")

  def slice(sampleData: SampleData): (Slice, SampleData) = {
    val (prefix, suffix) = sampleData.data.splitAt(sliceSize)

    @tailrec def buildColArrays(from: Stream[JValue], into: Map[ColumnRef, (BitSet, Array[_])], sliceIndex: Int): (Map[ColumnRef, (BitSet, Object)], Int) = {
      from match {
        case jv #:: xs =>
          val withIdsAndValues = jv.flattenWithPath.foldLeft(into) {
            case (acc, (jpath, JNothing)) => acc
            case (acc, (jpath, v)) =>
              val ctype = CType.forJValue(v) getOrElse { sys.error("Cannot determine ctype for " + v + " at " + jpath + " in " + jv) }
              val ref = ColumnRef(jpath, ctype)

              val pair: (BitSet, Array[_]) = v match {
                case JBool(b) => 
                  val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[Boolean](sliceSize))).asInstanceOf[(BitSet, Array[Boolean])]
                  col(sliceIndex) = b
                  (defined + sliceIndex, col)

                case JInt(ji) => CType.sizedIntCValue(ji) match {
                  case CLong(v) =>
                    val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[Long](sliceSize))).asInstanceOf[(BitSet, Array[Long])]
                    col(sliceIndex) = v
                    (defined + sliceIndex, col)

                  case CNum(v) =>
                    val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[BigDecimal](sliceSize))).asInstanceOf[(BitSet, Array[BigDecimal])]
                    col(sliceIndex) = v
                    (defined + sliceIndex, col)
                }

                case JDouble(d) => 
                  val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[Double](sliceSize))).asInstanceOf[(BitSet, Array[Double])]
                  col(sliceIndex) = d
                  (defined + sliceIndex, col)

                case JString(s) => 
                  val (defined, col) = acc.getOrElse(ref, (BitSet(), new Array[String](sliceSize))).asInstanceOf[(BitSet, Array[String])]
                  col(sliceIndex) = s
                  (defined + sliceIndex, col)
                
                case JArray(Nil)  => 
                  val (defined, col) = acc.getOrElse(ref, (BitSet(), null)).asInstanceOf[(BitSet, Array[Boolean])]
                  (defined + sliceIndex, col)

                case JObject(Nil) => 
                  val (defined, col) = acc.getOrElse(ref, (BitSet(), null)).asInstanceOf[(BitSet, Array[Boolean])]
                  (defined + sliceIndex, col)

                case JNull        => 
                  val (defined, col) = acc.getOrElse(ref, (BitSet(), null)).asInstanceOf[(BitSet, Array[Boolean])]
                  (defined + sliceIndex, col)
              }

              acc + (ref -> pair)
          }

          buildColArrays(xs, withIdsAndValues, sliceIndex + 1)

        case _ => (into, sliceIndex)
      }
    }

    val slice = new Slice {
      val (cols, size) = buildColArrays(prefix, Map.empty[ColumnRef, (BitSet, Array[_])], 0) 
      val columns = cols map {
        case (ref @ ColumnRef(_, CBoolean), (defined, values))          => (ref, ArrayBoolColumn(defined, values.asInstanceOf[Array[Boolean]]))
        case (ref @ ColumnRef(_, CLong), (defined, values))             => (ref, ArrayLongColumn(defined, values.asInstanceOf[Array[Long]]))
        case (ref @ ColumnRef(_, CDouble), (defined, values))           => (ref, ArrayDoubleColumn(defined, values.asInstanceOf[Array[Double]]))
        case (ref @ ColumnRef(_, CDecimalArbitrary), (defined, values)) => (ref, ArrayNumColumn(defined, values.asInstanceOf[Array[BigDecimal]]))
        case (ref @ ColumnRef(_, CStringArbitrary), (defined, values))  => (ref, ArrayStrColumn(defined, values.asInstanceOf[Array[String]]))
        case (ref @ ColumnRef(_, CEmptyArray), (defined, values))       => (ref, new BitsetColumn(defined) with EmptyArrayColumn)
        case (ref @ ColumnRef(_, CEmptyObject), (defined, values))      => (ref, new BitsetColumn(defined) with EmptyObjectColumn)
        case (ref @ ColumnRef(_, CNull), (defined, values))             => (ref, new BitsetColumn(defined) with NullColumn)
      }
    }

    (slice, SampleData(suffix))
  }

  def fromJson(sampleData: SampleData): Table = {
    val (s, xs) = spec.slice(sampleData)

    new Table(new Iterable[Slice] {
      def iterator = new Iterator[Slice] {
        private var _next = s
        private var _rest = xs

        def hasNext = _next != null
        def next() = {
          val tmp = _next
          _next = if (_rest.data.isEmpty) null else {
            val (s, xs) = spec.slice(_rest)
            _rest = xs
            s
          }
          tmp
        }
      }
    })
  }

  def toJson(dataset: Table): Stream[JValue] = {
    dataset.toJson.toStream
  }

  def cogroup(ds1: Table, ds2: Table): Table = sys.error("todo")

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

      val dataset = fromJson(SampleData(sample.toStream))
      //dataset.slices.foreach(println)
      val results = dataset.toJson.toList
      results must containAllOf(sample).only
    }

    "verify bijection from JSON" in checkMappings

    /*
    "in cogroup" >> {
      "survive scalacheck" in { 
        check { cogroupData: (SampleData, SampleData) => testCogroup(cogroupData._1, cogroupData._2) } 
      }

      "cogroup across slice boundaries" in testCogroupSliceBoundaries
      "survive pathology 2" in testCogroupPathology2
    }
    */

    "in transform" >> {
      "perform the identity transform" in checkTransformLeaf
      "perform a trivial map1" in testMap1IntLeaf
      //"give the identity transform for the trivial filter" in checkTrivialFilter
      "give the identity transform for the trivial 'true' filter" in checkTrueFilter
      "give the identity transform for a nontrivial filter" in checkFilter
      "perform an object dereference" in checkObjectDeref
      "perform a trivial map2" in checkMap2
    }
  }
}


// vim: set ts=4 sw=4 et:
