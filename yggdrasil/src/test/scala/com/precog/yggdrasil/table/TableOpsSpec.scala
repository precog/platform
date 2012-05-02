package com.precog.yggdrasil
package table

import com.precog.common.Path
import com.precog.common.VectorCase

import blueeyes.json.JsonAST._

import scala.annotation.tailrec

import org.specs2._
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

class TableOpsSpec extends DatasetOpsSpec { spec =>
  override val defaultPrettyParams = Pretty.Params(2)

  val sliceSize = 10
  val testPath = Path("/tableOpsSpec")

  def slice(sampleData: SampleData): (Slice, SampleData) = {
    val (prefix, suffix) = sampleData.data.splitAt(sliceSize)
    var i = 0
    val (ids, columns) = prefix.foldLeft((List.fill(sampleData.idCount)(ArrayColumn(CLong, sliceSize)), Map.empty[VColumnRef[_], Column[_]])) {
      case ((idsAcc, colAcc), (ids, jv)) =>
        for (j <- 0 until ids.length) idsAcc(j)(i) = ids(j)

        val newAcc = jv.flattenWithPath.foldLeft(colAcc) {
          case (acc, (jpath, v)) =>
            println("adding column value at " + i + ": " + (jpath, v))
            val ctype = CType.forJValue(v).get
            val ref = VColumnRef[ctype.CA](NamedColumnId(testPath, jpath), ctype)

            val arr: Column[_] = v match {
              case JString(s) => 
                val arr: ArrayColumn[String] = acc.getOrElse(ref, ArrayColumn(CStringArbitrary, sliceSize)).asInstanceOf[ArrayColumn[String]]
                arr(i) = s
                arr
              
              case JInt(ji) => CType.sizedIntCValue(ji) match {
                case CInt(v) => 
                  val arr: ArrayColumn[Int] = acc.getOrElse(ref, ArrayColumn(CInt, sliceSize)).asInstanceOf[ArrayColumn[Int]]
                  arr(i) = v
                  arr

                case CLong(v) =>
                  val arr: ArrayColumn[Long] = acc.getOrElse(ref, ArrayColumn(CLong, sliceSize)).asInstanceOf[ArrayColumn[Long]]
                  arr(i) = v
                  arr

                case CNum(v) =>
                  val arr: ArrayColumn[BigDecimal] = acc.getOrElse(ref, ArrayColumn(CDecimalArbitrary, sliceSize)).asInstanceOf[ArrayColumn[BigDecimal]]
                  arr(i) = v
                  arr
              }

              case JDouble(d) => 
                val arr: ArrayColumn[Double] = acc.getOrElse(ref, ArrayColumn(CDouble, sliceSize)).asInstanceOf[ArrayColumn[Double]]
                arr(i) = d
                arr

              case JBool(b) => 
                val arr: ArrayColumn[Boolean] = acc.getOrElse(ref, ArrayColumn(CBoolean, sliceSize)).asInstanceOf[ArrayColumn[Boolean]]
                arr(i) = b
                arr

              case JArray(Nil)  => Column.const(CEmptyArray, null)
              case JObject(Nil) => Column.const(CEmptyObject, null)
              case JNull        => Column.const(CNull, null)
            }

            acc + (ref -> arr)
        }

        i += 1

        (idsAcc, newAcc)
    }

    val result = Slice(ids, columns mapValues { case c: ArrayColumn[_] => c.resize(i); case x => x }, i)

    result.columns.foreach(println)

    for (row <- 0 until result.size) println("r: " + result.toJson(row))

    (result, SampleData(sampleData.idCount, suffix))
  }

  def fromJson(sampleData: SampleData): Table = {
    val (s, xs) = spec.slice(sampleData)

    new Table(sampleData.idCount, s.columns.keySet, new Iterable[Slice] {
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

      sample foreach { case (ids, v) => println("s: " + v) }

      val dataset = fromJson(SampleData(2, sample.toStream))
      val results = dataset.toEvents.toList
      results must containAllOf(sample).only
    }

    //"verify bijection from JSON" in checkMappings
    //"cogroup" in checkCogroup
  }
}


// vim: set ts=4 sw=4 et:
