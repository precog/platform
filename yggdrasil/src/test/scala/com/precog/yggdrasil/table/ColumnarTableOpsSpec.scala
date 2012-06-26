package com.precog.yggdrasil
package table

import com.precog.common.Path
import com.precog.common.VectorCase

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser

import scala.annotation.tailrec
import scalaz._

import org.specs2._
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

class TableOpsSpec extends TableModuleSpec with CogroupSpec with ColumnarTableModule { spec =>
  override val defaultPrettyParams = Pretty.Params(2)

  val sliceSize = 10
  val testPath = Path("/tableOpsSpec")

  def debugPrint(dataset: Table): Unit = {
    println("\n\n")
    for (slice <- dataset.slices; i <- 0 until slice.size) println(slice.toString(i))
  }

  def cogroup(ds1: Table, ds2: Table): Table = {
    sys.error("todo")
    //ds1.cogroup(ds2, ds1.idCount min ds2.idCount)(CogroupMerge.second)
  }

  def slice(sampleData: SampleData): (Slice, SampleData) = {
    val (prefix, suffix) = sampleData.data.splitAt(sliceSize)

    @tailrec def buildColArrays(from: Stream[Record[JValue]], into: Map[ColumnRef, Array[_]], sliceIndex: Int): (Map[ColumnRef, Object], Int) = {
      from match {
        case (ids, jv) #:: xs =>
          val withIds: Map[ColumnRef, Array[_]] = (0 until ids.length).foldLeft(into) {
            case (acc, j) => 
              val cref = ColumnRef(JPath(JPathField("keys") :: JPathIndex(j) :: Nil: _*), CLong)
              val arr: Array[Long] = acc.getOrElse(cref, new Array[Long](sliceSize)).asInstanceOf[Array[Long]]
              arr(j) = ids(j)
              acc + (cref -> arr)
          }

          val withIdsAndValues = jv.flattenWithPath.foldLeft(withIds) {
            case (acc, (jpath, JNothing)) => acc
            case (acc, (jpath, v)) =>
              val ctype = CType.forJValue(v) getOrElse { sys.error("Cannot determine ctype for " + v + " at " + jpath + " in " + jv) }
              val ref = ColumnRef(jpath, ctype)

              val col: Array[_] = v match {
                case JBool(b) => 
                  val col: Array[Boolean] = acc.getOrElse(ref, new Array[Boolean](sliceSize)).asInstanceOf[Array[Boolean]]
                  col(sliceIndex) = b
                  col

                case JInt(ji) => CType.sizedIntCValue(ji) match {
                  case CLong(v) =>
                    val col: Array[Long] = acc.getOrElse(ref, new Array[Long](sliceSize)).asInstanceOf[Array[Long]]
                    col(sliceIndex) = v
                    col

                  case CNum(v) =>
                    val col: Array[BigDecimal] = acc.getOrElse(ref, new Array[BigDecimal](sliceSize)).asInstanceOf[Array[BigDecimal]]
                    col(sliceIndex) = v
                    col
                }

                case JDouble(d) => 
                  val col: Array[Double] = acc.getOrElse(ref, new Array[Double](sliceSize)).asInstanceOf[Array[Double]]
                  col(sliceIndex) = d
                  col

                case JString(s) => 
                  val col: Array[String] = acc.getOrElse(ref, new Array[String](sliceSize)).asInstanceOf[Array[String]]
                  col(sliceIndex) = s
                  col
                
                case JArray(Nil)  => 
                  val col: Array[Boolean] = acc.getOrElse(ref, new Array[Boolean](sliceSize)).asInstanceOf[Array[Boolean]]
                  col(sliceIndex) = true
                  col

                case JObject(Nil) => 
                  val col: Array[Boolean] = acc.getOrElse(ref, new Array[Boolean](sliceSize)).asInstanceOf[Array[Boolean]]
                  col(sliceIndex) = true
                  col

                case JNull        => 
                  val col: Array[Boolean] = acc.getOrElse(ref, new Array[Boolean](sliceSize)).asInstanceOf[Array[Boolean]]
                  col(sliceIndex) = true
                  col
              }

              acc + (ref -> col)
          }

          buildColArrays(xs, withIdsAndValues, sliceIndex + 1)

        case _ => (into, sliceIndex)
      }
    }

    val slice = new Slice {
      val (cols, size) = buildColArrays(prefix, Map.empty[ColumnRef, Array[_]], 0) 
      val columns = cols map {
        case (ref @ ColumnRef(_, CBoolean), values) => (ref, ArrayBoolColumn(values.asInstanceOf[Array[Boolean]]))
        case (ref @ ColumnRef(_, CLong), values)    => (ref, ArrayLongColumn(values.asInstanceOf[Array[Long]]))
        case (ref @ ColumnRef(_, CDouble), values)  => (ref, ArrayDoubleColumn(values.asInstanceOf[Array[Double]]))
        case (ref @ ColumnRef(_, CDecimalArbitrary), values) => (ref, ArrayNumColumn(values.asInstanceOf[Array[BigDecimal]]))
        case (ref @ ColumnRef(_, CStringArbitrary), values)  => (ref, ArrayStrColumn(values.asInstanceOf[Array[String]]))
        case (ref @ ColumnRef(_, CEmptyArray), values)  => (ref, new BitsetColumn(BitsetColumn.bitset(values.asInstanceOf[Array[Boolean]])) with EmptyArrayColumn)
        case (ref @ ColumnRef(_, CEmptyObject), values) => (ref, new BitsetColumn(BitsetColumn.bitset(values.asInstanceOf[Array[Boolean]])) with EmptyObjectColumn)
        case (ref @ ColumnRef(_, CNull), values)        => (ref, new BitsetColumn(BitsetColumn.bitset(values.asInstanceOf[Array[Boolean]])) with NullColumn)
      }
    }

    (slice, SampleData(sampleData.idCount, suffix))
  }

  def fromJson(sampleData: SampleData): Table = {
    val (s, xs) = spec.slice(sampleData)

    new Table(s.columns.keySet, new Iterable[Slice] {
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

  def toJson(dataset: Table): Stream[Record[JValue]] = {
    dataset.toEvents.toStream
  }

  def toValidatedJson(dataset: Table): Stream[Record[ValidationNEL[Throwable, JValue]]] = {
    dataset.toValidatedEvents.toStream
  }

  "a table dataset" should {
    "verify bijection from static JSON" in {
      val sample: List[(Identities, JValue)] = List(
        (VectorCase(-1l, 0l),JNull), 
        (VectorCase(-3090012080927607325l, 2875286661755661474l),
          JObject(List(
            JField("q8b", JArray(List(
              JDouble(6.615224799778253E307d), 
              JArray(List(JBool(false), JNull, JDouble(-8.988465674311579E307d))), JDouble(-3.536399224770604E307d)))), 
            JField("lwu",JDouble(-5.121099465699862E307d))))), 
        (VectorCase(-3918416808128018609l, 1l),JDouble(-1.0))
      )

      val dataset = fromJson(SampleData(2, sample.toStream))
      val results = dataset.toEvents.toList
      results must containAllOf(sample).only
    }

    "verify bijection from JSON" in checkMappings
    "in cogroup" >> {
      "survive scalacheck" in { 
        check { cogroupData: (SampleData, SampleData) => testCogroup(cogroupData._1, cogroupData._2) } 
      }

      "cogroup across slice boundaries" in testCogroupSliceBoundaries
      "survive pathology 2" in testCogroupPathology2
    }


  }
}


// vim: set ts=4 sw=4 et:
