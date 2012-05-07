package com.precog.yggdrasil

import table._
import com.precog.common.VectorCase
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser

import scalaz.{NonEmptyList => NEL, _}
import scalaz.Ordering._
import scalaz.Either3._
import scalaz.std.tuple._
import scala.annotation.tailrec

import org.specs2._
import org.specs2.mutable.Specification
import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

trait DatasetOpsSpec extends Specification with ScalaCheck with SValueGenerators {
  type Dataset = Table
  type Record[A] = (Identities, A)

  implicit def order[A] = tupledIdentitiesOrder[A]()

  case class SampleData(idCount: Int, data: Stream[Record[JValue]]) {
    override def toString = {
      "\nSampleData: \nidCount = "+idCount+",\ndata = "+
      data.map({ case (ids, v) => ids.mkString("(", ",", ")") + ": " + v.toString.replaceAll("\n", "\n  ") }).mkString("[\n  ", ",\n  ", "]\n")
    }
  }

  def fromJson(sampleData: SampleData): Dataset
  def toJson(dataset: Dataset): Stream[Record[JValue]]
  def toValidatedJson(dataset: Dataset): Stream[Record[ValidationNEL[Throwable, JValue]]]

  def normalizeValidations(s: Stream[Record[ValidationNEL[Throwable, JValue]]]): Stream[Record[Option[JValue]]] = {
    s map {
      case (ids, Failure(t)) => println(t.list.mkString(";\n")); (ids, None)
      case (ids, Success(v)) => (ids, Some(v))
    }
  }

  implicit def identitiesOrdering = IdentitiesOrder.toScalaOrdering

  implicit val arbData = Arbitrary(
    for {
      idCount <- choose(1, 3) 
      data <- containerOf[List, Record[JValue]](sevent(idCount, 3) map { case (ids, sv) => (ids, sv.toJValue) })
    } yield {
      SampleData(idCount, data.sortBy(_._1).toStream)
    }
  )

  def checkMappings = {
    check { (sample: SampleData) =>
      val dataset = fromJson(sample)
      dataset.toEvents.toList must containAllOf(sample.data.toList).only
    }
  }

  type CogroupResult[A] = Stream[Record[Either3[A, (A, A), A]]]
  @tailrec protected final def computeCogroup[A](l: Stream[Record[A]], r: Stream[Record[A]], acc: CogroupResult[A], idPrefix: Int)(implicit ord: Order[Record[A]]): CogroupResult[A] = {
    (l, r) match {
      case (lh #:: lt, rh #:: rt) => ord.order(lh, rh) match {
        case EQ => {
          val (leftSpan, leftRemain) = l.partition(ord.order(_, lh) == EQ)
          val (rightSpan, rightRemain) = r.partition(ord.order(_, rh) == EQ)

          val cartesian = leftSpan.flatMap { case (idl, lv) => rightSpan.map { case (idr, rv) => (idl ++ idr.drop(idPrefix), middle3((lv, rv))) } }

          computeCogroup(leftRemain, rightRemain, acc ++ cartesian, idPrefix)
        }
        case LT => {
          val (leftRun, leftRemain) = l.partition(ord.order(_, rh) == LT)
          
          computeCogroup(leftRemain, r, acc ++ leftRun.map { case (i, v) => (i, left3(v)) }, idPrefix)
        }
        case GT => {
          val (rightRun, rightRemain) = r.partition(ord.order(lh, _) == GT)

          computeCogroup(l, rightRemain, acc ++ rightRun.map { case (i, v) => (i, right3(v)) }, idPrefix)
        }
      }
      case (Stream.Empty, _) => acc ++ r.map { case (i,v) => (i, right3(v)) }
      case (_, Stream.Empty) => acc ++ l.map { case (i,v) => (i, left3(v)) }
    }
  }

  def testCogroup(l: SampleData, r: SampleData) = {
    val idCount = l.idCount max r.idCount

    val expected = computeCogroup(l.data, r.data, Stream(), l.idCount min r.idCount) map {
      case (ids, Left3(jv)) => (ids, Validation.success[NEL[Throwable], JValue](jv))
      case (ids, Middle3((jv1, jv2))) => (ids, jv1.insertAll(jv2))
      case (ids, Right3(jv)) => (ids, Validation.success[NEL[Throwable], JValue](jv))
    } map {
      case (ids, jv) => (VectorCase(ids.padTo(idCount, -1l): _*), jv)
    }

    val ltable = fromJson(l)
    val rtable = fromJson(r)
    val result = toValidatedJson(ltable.cogroup(rtable, ltable.idCount min rtable.idCount)(CogroupMerge.second))

    normalizeValidations(result) must containAllOf(normalizeValidations(expected)).only.inOrder
  }

  def testCogroupPathology1 = {
    val sample1 = SampleData(1, Stream(
      (VectorCase(4611686018427387903L),JObject(List())), 
      (VectorCase(-4611686018427387904L),JNull), 
      (VectorCase(-4611686018427387904L),JObject(List())), 
      (VectorCase(-4611686018427387904L),JObject(List(JField("djv",JArray(List())))))
    ))
    
    val sample2 = SampleData(1,Stream())

    testCogroup(sample1, sample2)
  }

  def testCogroupPathology2 = {
    val sample1 = SampleData(0,Stream(
      (VectorCase(), JArray(List(
        JNull, 
        JDouble(0.0), 
        JObject(List(
          JField("g",JArray(List(JNull, JString("dx"), JDouble(2.1197438388263529E18)))), 
          JField("wn",
            JObject(List(
              JField("clw",JDouble(-5.177567408175664E307)), 
              JField("r",JString("qh")))))
        ))
      ))), 
      (VectorCase(),JArray(List(
        JString("sr"), 
        JArray(List(
          JObject(List(
            JField("keh",JDouble(4.4461190557968074E307)), 
            JField("fm",JString("pe")), 
            JField("z",JDouble(1.20904848757511344E17)))
          ), 
          JObject(List(
            JField("hq",JBool(true)), 
            JField("m",JDouble(0.0)))
          ), 
          JDouble(-8.988465674311579E307))
        ), 
        JDouble(5.058352168854163E307)
      )))
    )) 
    
    val sample2 = SampleData(2,Stream(
      (VectorCase(-4611686018427387904l, 0l),JArray(List(JArray(List(JArray(List()), JObject(List()))))))
    ))

    testCogroup(sample1, sample2)
  }

  def testCogroupPathology3 = {
    val sample1 = SampleData(3, Stream(
      VectorCase(-4565581309667628497l, 4611686018427387903l, 448738091425882593l) -> JString("Vt"), 
      VectorCase(3557578357066956826l, 3721915890484749616l, 0l) -> JsonParser.parse("""{
        "fnst":[0.0,[]],
        "oo":"",
        "dubu":{
          "moc":[-4.6116860184273879E18,4.6116860184273879E18],
          "q":{ },
          "v4bbR":{
            "gcjsh":-4.6116860184273879E18
          }
        }
      }""")
    ))
    
    val sample2 = SampleData(1,Stream(
      VectorCase(4442433569090792463l) -> JObject(Nil), 
      VectorCase(-611677476198273678l) -> JString("icrw")
    ))

    testCogroup(sample1, sample2)
  }

  def testCogroupPathology4 = {
    val sample1 = SampleData(
      idCount = 3,
      data = Stream(
        VectorCase(1l,1l,1l) -> JsonParser.parse("""{ "e":{ "hl": 1.0 }, "fr": 2.0 }""")
      )
    )

    val sample2 = SampleData(
      idCount = 1,
      data = Stream(
        VectorCase(1l) -> JsonParser.parse("""3.0""")
      )
    )

    testCogroup(sample1, sample2)
  }

  def testCogroupPathology5 = {
    val sample1 = SampleData(
      idCount = 2,
      data = Stream(
        VectorCase(1L,2L) -> JArray(Nil),
        VectorCase(2L,2L) -> JArray(Nil)
      )
    )
    
    val sample2 = SampleData(
      idCount = 1,
      data = Stream(
        VectorCase(1L) -> JsonParser.parse("""[[], { "a": { }, "b": [null, null] }]""")
      )
    )

    testCogroup(sample1, sample2)
  }
}

// vim: set ts=4 sw=4 et:
