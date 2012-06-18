package com.precog.yggdrasil
package table

import com.precog.common.Path
import com.precog.common.VectorCase

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

class TableOpsSpec extends DatasetOpsSpec { spec =>
  type Dataset = Table

  override val defaultPrettyParams = Pretty.Params(2)

  val sliceSize = 10
  val testPath = Path("/tableOpsSpec")

  def debugPrint(dataset: Table): Unit = {
    println("\n\n")
    for (slice <- dataset.slices; i <- 0 until slice.size) println(slice.toString(i))
  }

  def cogroup(ds1: Dataset, ds2: Dataset): Dataset = {
    ds1.cogroup(ds2, ds1.idCount min ds2.idCount)(CogroupMerge.second)
  }
  

  def slice(sampleData: SampleData): (Slice, SampleData) = {
    val (prefix, suffix) = sampleData.data.splitAt(sliceSize)
    var i = 0
    val (ids, columns) = prefix.foldLeft((List.fill(sampleData.idCount)(ArrayColumn(CLong, sliceSize)), Map.empty[VColumnRef[_], Column[_]])) {
      case ((idsAcc, colAcc), (ids, jv)) =>
        for (j <- 0 until ids.length) idsAcc(j)(i) = ids(j)

        val newAcc = jv.flattenWithPath.foldLeft(colAcc) {
          case (acc, (jpath, JNothing)) => acc
          case (acc, (jpath, v)) =>
            val ctype = CType.forJValue(v) getOrElse {
              sys.error("Cannot determine ctype for " + v + " at " + jpath + " in " + jv)
            }

            val ref = VColumnRef[ctype.CA](NamedColumnId(testPath, jpath), ctype)

            val col: Column[_] = v match {
              case JString(s) => 
                val col: ArrayColumn[String] = acc.getOrElse(ref, ArrayColumn(CStringArbitrary, sliceSize)).asInstanceOf[ArrayColumn[String]]
                col(i) = s
                col
              
              case JInt(ji) => CType.sizedIntCValue(ji) match {
                case CInt(v) => 
                  val col: ArrayColumn[Int] = acc.getOrElse(ref, ArrayColumn(CInt, sliceSize)).asInstanceOf[ArrayColumn[Int]]
                  col(i) = v
                  col

                case CLong(v) =>
                  val col: ArrayColumn[Long] = acc.getOrElse(ref, ArrayColumn(CLong, sliceSize)).asInstanceOf[ArrayColumn[Long]]
                  col(i) = v
                  col

                case CNum(v) =>
                  val col: ArrayColumn[BigDecimal] = acc.getOrElse(ref, ArrayColumn(CDecimalArbitrary, sliceSize)).asInstanceOf[ArrayColumn[BigDecimal]]
                  col(i) = v
                  col
              }

              case JDouble(d) => 
                val col: ArrayColumn[Double] = acc.getOrElse(ref, ArrayColumn(CDouble, sliceSize)).asInstanceOf[ArrayColumn[Double]]
                col(i) = d
                col

              case JBool(b) => 
                val col: ArrayColumn[Boolean] = acc.getOrElse(ref, ArrayColumn(CBoolean, sliceSize)).asInstanceOf[ArrayColumn[Boolean]]
                col(i) = b
                col

              case JArray(Nil)  => 
                val col = acc.getOrElse(ref, new CEmptyArrayColumn(sliceSize)).asInstanceOf[NullColumn]
                col.defined(i) = true
                col

              case JObject(Nil) => 
                val col = acc.getOrElse(ref, new CEmptyObjectColumn(sliceSize)).asInstanceOf[NullColumn]
                col.defined(i) = true
                col

              case JNull        => 
                val col = acc.getOrElse(ref, new CNullColumn(sliceSize)).asInstanceOf[NullColumn]
                col.defined(i) = true
                col
            }

            acc + (ref -> col)
        }

        i += 1

        (idsAcc, newAcc)
    }

    val result = Slice(ids, columns mapValues { case c: ArrayColumn[_] => c.prefix(i); case x => x }, i)

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
