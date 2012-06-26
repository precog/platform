package com.precog.yggdrasil

import table._
import com.precog.common.VectorCase
import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser

import scalaz.{NonEmptyList => NEL, _}
import scalaz.BiFunctor
import scalaz.Ordering._
import scalaz.Either3._
import scalaz.std.tuple._
import scalaz.std.function._
import scalaz.syntax.arrow._
import scalaz.syntax.biFunctor._
import scala.annotation.tailrec

import org.specs2._
import org.specs2.mutable.Specification
import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

trait CogroupSpec extends TableModuleSpec {
  implicit val cogroupData = Arbitrary(
    for {
      depth   <- choose(1, 2)
      jschema  <- Gen.oneOf(arraySchema(depth, 2), objectSchema(depth, 2))
      (idCount, data) <- genEventColumns(jschema)
    } yield {
      val (lschema, rschema) = BiFunctor[Tuple2].umap(jschema.splitAt(jschema.size / 2)) { _.map(_._1).toSet }
      val (l, r) =  data map {
                      case (ids, values) => 
                        val (d1, d2) = values.partition { case (jpath, _) => lschema.contains(jpath) }
                        (ids, assemble(d1), assemble(d2))
                    } map {
                      case (ids, v1, v2) => ((ids, v1), (ids, v2))
                    } unzip

      (SampleData(idCount, l.sortBy(_._1).toStream), SampleData(idCount, r.sortBy(_._1).toStream))
    }
  )

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

  def cogroup(ds1: Table, ds2: Table): Table

  def testCogroup(l: SampleData, r: SampleData) = {
    try {
      val idCount = l.idCount max r.idCount

      val ltable = fromJson(l)
      val rtable = fromJson(r)

      val expected = computeCogroup(l.data, r.data, Stream(), l.idCount min r.idCount) map {
        case (ids, Left3(jv)) => (ids, jv)
        case (ids, Middle3((jv1, jv2))) => (ids, jv1.insertAll(jv2) match { case Success(v) => v; case Failure(ts) => throw ts.head })
        case (ids, Right3(jv)) => (ids, jv)
      } map {
        case (ids, jv) => (VectorCase(ids.padTo(idCount, -1l): _*), jv)
      }

      val result = cogroup(ltable, rtable)
      val jsonResult = toJson(result)
      jsonResult must containAllOf(expected).only
    } catch {
      case ex => ex.printStackTrace; throw ex
    } 

//    val expected = computeCogroup(l.data, r.data, Stream(), l.idCount min r.idCount) map {
//      case (ids, Left3(jv)) => (ids, Validation.success[NEL[Throwable], JValue](jv))
//      case (ids, Middle3((jv1, jv2))) => (ids, jv1.insertAll(jv2))
//      case (ids, Right3(jv)) => (ids, Validation.success[NEL[Throwable], JValue](jv))
//    } map {
//      case (ids, jv) => (VectorCase(ids.padTo(idCount, -1l): _*), jv)
//    }
//
//    val result = toValidatedJson(ltable.cogroup(rtable, ltable.idCount min rtable.idCount)(CogroupMerge.second))
//    normalizeValidations(result) must containAllOf(normalizeValidations(expected)).only.inOrder
  }

  def testCogroupSliceBoundaries = {
    import JsonParser.parse

    val s1 = SampleData(1, Stream(
      (VectorCase(1), parse("""{ "ruoh5A25Jaxa":-1.0 }""")),
      (VectorCase(2), parse("""{ "ruoh5A25Jaxa":-2.735023101944097E37 }""")),
      (VectorCase(3), parse("""{ "ruoh5A25Jaxa":2.12274644226519E38 }""")),
      (VectorCase(4), parse("""{ "ruoh5A25Jaxa":1.085656944502855E38 }""")),
      (VectorCase(5), parse("""{ "ruoh5A25Jaxa":-3.4028234663852886E38 }""")),
      (VectorCase(6), parse("""{ "ruoh5A25Jaxa":-1.0 }""")),
      (VectorCase(7), parse("""{ "ruoh5A25Jaxa":-3.4028234663852886E38 }""")),
      (VectorCase(8), parse("""{ "ruoh5A25Jaxa":2.4225587899613125E38 }""")),
      (VectorCase(9), parse("""{ "ruoh5A25Jaxa":-3.078101074510345E38 }""")),
      (VectorCase(10), parse("""{ "ruoh5A25Jaxa":0.0 }""")),
      (VectorCase(11), parse("""{ "ruoh5A25Jaxa":-2.049657967962047E38 }"""))
    ))

    val s2 = SampleData(1, Stream(
      (VectorCase(1), parse("""{ "mbsn8ya":-629648309198725501 }""")),
      (VectorCase(2), parse("""{ "mbsn8ya":-1642079669762657762 }""")),
      (VectorCase(3), parse("""{ "mbsn8ya":-75462980385303464 }""")),
      (VectorCase(4), parse("""{ "mbsn8ya":-4407493923710190330 }""")),
      (VectorCase(5), parse("""{ "mbsn8ya":4611686018427387903 }""")),
      (VectorCase(6), parse("""{ "mbsn8ya":-4374327062386862583 }""")),
      (VectorCase(7), parse("""{ "mbsn8ya":1920642186250198767 }""")),
      (VectorCase(8), parse("""{ "mbsn8ya":1 }""")),
      (VectorCase(9), parse("""{ "mbsn8ya":0 }""")),
      (VectorCase(10), parse("""{ "mbsn8ya":1 }""")),
      (VectorCase(11), parse("""{ "mbsn8ya":758880641626989193 }"""))
    ))

    testCogroup(s1, s2)
  }

  def testCogroupPathology2 = {
    val s1 = SampleData(3, Stream(
      (VectorCase(19,49,71), JArray(JInt(-4611686018427387904l) :: Nil)),
      (VectorCase(28,15,27), JArray(JInt(-4611686018427387904l) :: Nil)),
      (VectorCase(33,11,79), JArray(JInt(-1330862996622233403l) :: Nil)),
      (VectorCase(38,9,3),   JArray(JInt(483746605685223474l) :: Nil)),
      (VectorCase(44,75,87), JArray(JInt(4611686018427387903l) :: Nil)),
      (VectorCase(46,47,10), JArray(JInt(-4611686018427387904l) :: Nil)),
      (VectorCase(47,17,78), JArray(JInt(3385965380985908250l) :: Nil)),
      (VectorCase(47,89,84), JArray(JInt(-3713232335731560170l) :: Nil)),
      (VectorCase(48,47,76), JArray(JInt(4611686018427387903l) :: Nil)),
      (VectorCase(49,66,33), JArray(JInt(-1592288472435607010l) :: Nil)),
      (VectorCase(50,9,89),  JArray(JInt(-3610518022153967388l) :: Nil)),
      (VectorCase(59,54,72), JArray(JInt(4178019033671378504l) :: Nil)),
      (VectorCase(59,80,38), JArray(JInt(0) :: Nil)),
      (VectorCase(61,59,15), JArray(JInt(1056424478602208129l) :: Nil)),
      (VectorCase(65,34,89), JArray(JInt(4611686018427387903l) :: Nil)),
      (VectorCase(73,52,67), JArray(JInt(-4611686018427387904l) :: Nil)),
      (VectorCase(74,60,85), JArray(JInt(-4477191148386604184l) :: Nil)),
      (VectorCase(76,41,86), JArray(JInt(-2686421995147680512l) :: Nil)),
      (VectorCase(77,46,75), JArray(JInt(-1) :: Nil)),
      (VectorCase(77,65,58), JArray(JInt(-4032275398385636682l) :: Nil)),
      (VectorCase(86,50,9),  JArray(JInt(4163435383002324073l) :: Nil))
    ))

    val s2 = SampleData(3, Stream(
      (VectorCase(19,49,71), JArray(JNothing :: JDouble(2.2447601450142614E38) :: Nil)),
      (VectorCase(28,15,27), JArray(JNothing :: JDouble(-1.0) :: Nil)),
      (VectorCase(33,11,79), JArray(JNothing :: JDouble(-3.4028234663852886E38) :: Nil)),
      (VectorCase(38,9,3),   JArray(JNothing :: JDouble(3.4028234663852886E38) :: Nil)),
      (VectorCase(44,75,87), JArray(JNothing :: JDouble(3.4028234663852886E38) :: Nil)),
      (VectorCase(46,47,10), JArray(JNothing :: JDouble(-7.090379511750481E37) :: Nil)),
      (VectorCase(47,17,78), JArray(JNothing :: JDouble(2.646265046453461E38) :: Nil)),
      (VectorCase(47,89,84), JArray(JNothing :: JDouble(0.0) :: Nil)),
      (VectorCase(48,47,76), JArray(JNothing :: JDouble(1.3605700991092947E38) :: Nil)),
      (VectorCase(49,66,33), JArray(JNothing :: JDouble(-1.4787158449349019E38) :: Nil)),
      (VectorCase(50,9,89),  JArray(JNothing :: JDouble(-1.0) :: Nil)),
      (VectorCase(59,54,72), JArray(JNothing :: JDouble(-3.4028234663852886E38) :: Nil)),
      (VectorCase(59,80,38), JArray(JNothing :: JDouble(8.51654525599509E37) :: Nil)),
      (VectorCase(61,59,15), JArray(JNothing :: JDouble(3.4028234663852886E38) :: Nil)),
      (VectorCase(65,34,89), JArray(JNothing :: JDouble(-1.0) :: Nil)),
      (VectorCase(73,52,67), JArray(JNothing :: JDouble(5.692401753312787E37) :: Nil)),
      (VectorCase(74,60,85), JArray(JNothing :: JDouble(2.5390881291535566E38) :: Nil)),
      (VectorCase(76,41,86), JArray(JNothing :: JDouble(-6.05866505535721E37) :: Nil)),
      (VectorCase(77,46,75), JArray(JNothing :: JDouble(0.0) :: Nil)),
      (VectorCase(77,65,58), JArray(JNothing :: JDouble(1.0) :: Nil)),
      (VectorCase(86,50,9),  JArray(JNothing :: JDouble(-3.4028234663852886E38) :: Nil))
    ))

    testCogroup(s1, s2)
  }
}


// vim: set ts=4 sw=4 et:
