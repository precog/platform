package com.precog.yggdrasil
package table

import com.precog.common.Path

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
    val (ids, columns) = prefix.foldLeft((List.fill(sampleData.idCount)(ArrayColumn(CLong, sliceSize)), Map.empty[VColumnRef[_], ArrayColumn[_]])) {
      case ((idsAcc, colAcc), (ids, jv)) =>
        for (j <- 0 until ids.length) idsAcc(j)(i) = ids(j)

        val newAcc = jv.flattenWithPath.foldLeft(colAcc) {
          case (acc, (jpath, v)) =>
            val ctype = CType.forJValue(v).get
            val ref = VColumnRef[ctype.CA](NamedColumnId(testPath, jpath), ctype)

            val arr: ArrayColumn[_] = v match {
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

              case _ => null
            }

            if (arr == null) acc else acc + (ref -> arr)
        }

        i += 1

        (idsAcc, newAcc)
    }

    (Slice(ids, columns mapValues { _.resize(i) }, i), SampleData(sampleData.idCount, suffix))
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
    "cogroup" in checkCogroup
  }
}


// vim: set ts=4 sw=4 et:
