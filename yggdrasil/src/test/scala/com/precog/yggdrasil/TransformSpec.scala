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

import akka.dispatch.Await
import akka.util.Duration

import blueeyes.json._
import blueeyes.json.JsonAST._

import org.specs2.mutable._

import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

import com.precog.bytecode._
import scala.util.Random
import scalaz.syntax.copointed._

trait TransformSpec[M[+_]] extends TableModuleSpec[M] {
  import SampleData._
  import trans._

  val resultTimeout = Duration(5, "seconds")

  def checkTransformLeaf = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val results = toJson(table.transform(Leaf(Source)))

      results.copoint must_== sample.data
    }
  }

  def testMap1IntLeaf = {
    val sample = (-10 to 10).map(JNum(_)).toStream
    val table = fromSample(SampleData(sample))
    val results = toJson(table.transform { Map1(Leaf(Source), lookupF1(Nil, "negate")) })

    results.copoint must_== (-10 to 10).map(x => JNum(-x))
  }

  /* Do we want to allow non-boolean sets to be used as filters without an explicit existence predicate?
  def checkTrivialFilter = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val results = toJson(table.transform {
        Filter(
          Leaf(Source), 
          Leaf(Source)
        )
      })

      results.copoint must_== sample.data
    }
  }
  */

  def checkTrueFilter = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val results = toJson(table.transform {
        Filter(
          Leaf(Source), 
          Equal(Leaf(Source), Leaf(Source))   //Map1 now makes undefined all columns not a the root-identity level
        )
      })

      results.copoint must_== sample.data
    }
  }

  def checkFilter = {
    implicit val gen = sample(_ => Gen.value(Seq(JPath.Identity -> CLong)))
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val results = toJson(table.transform {
        Filter(
          Leaf(Source), 
          Map1(
            DerefObjectStatic(Leaf(Source), JPathField("value")), 
            lookupF2(Nil, "mod").applyr(CLong(2)) andThen lookupF2(Nil, "eq").applyr(CLong(0))
          )
        )
      })

      val expected = sample.data flatMap { jv =>
        (jv \ "value") match { 
          case JNum(x) if x.longValue % 2 == 0 => Some(jv)
          case _ => None
        }
      }

      results.copoint must_== expected
    }
  }

  def checkObjectDeref = {
    implicit val gen = sample(objectSchema(_, 3))
    check { (sample: SampleData) =>
      val (field, _) = sample.schema.get._2.head
      val fieldHead = field.head.get
      val table = fromSample(sample)
      val results = toJson(table.transform {
        DerefObjectStatic(Leaf(Source), fieldHead.asInstanceOf[JPathField])
      })

      val expected = sample.data.map { jv => jv(JPath(fieldHead)) } flatMap {
        case JNothing => None
        case jv       => Some(jv)
      }

      results.copoint must_== expected
    }
  }

  def checkArrayDeref = {
    implicit val gen = sample(arraySchema(_, 3))
    check { (sample: SampleData) =>
      val (field, _) = sample.schema.get._2.head
      val fieldHead = field.head.get
      val table = fromSample(sample)
      val results = toJson(table.transform {
        DerefArrayStatic(Leaf(Source), fieldHead.asInstanceOf[JPathIndex])
      })

      val expected = sample.data.map { jv => jv(JPath(fieldHead)) } flatMap {
        case JNothing => None
        case jv       => Some(jv)
      }

      results.copoint must_== expected
    }
  }

  def checkMap2 = {
    implicit val gen = sample(_ => Seq(JPath("value1") -> CLong, JPath("value2") -> CLong))
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val results = toJson(table.transform {
        Map2(
          DerefObjectStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathField("value1")),
          DerefObjectStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathField("value2")),
          lookupF2(Nil, "add")
        )
      })

      val expected = sample.data flatMap { jv =>
        ((jv \ "value" \ "value1"), (jv \ "value" \ "value2")) match {
          case (JNum(x), JNum(y)) => Some(JNum(x+y))
          case _ => None
        }
      }

      results.copoint must_== expected
    }
  }

  def checkEqualSelf = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val results = toJson(table.transform {
        Equal(Leaf(Source), Leaf(Source))
      })

      results.copoint must_== (Stream.tabulate(sample.data.size) { _ => JBool(true) })
    }
  }

  def checkEqual = {
    val genBase: Gen[SampleData] = sample(_ => Seq(JPath("value1") -> CLong, JPath("value2") -> CLong)).arbitrary
    implicit val gen: Arbitrary[SampleData] = Arbitrary {
      genBase map { sd =>
        SampleData(
          sd.data.zipWithIndex map {
            case (jv, i) if i%2 == 0 => 
              // construct object with value1 == value2
              jv.set(JPath("value/value2"), jv(JPath("value/value1")))

            case (jv, i) if i%5 == 0 => // delete value1
              jv.set(JPath("value/value1"), JNothing)

            case (jv, i) if i%5 == 3 => // delete value2
              jv.set(JPath("value/value2"), JNothing)

            case (jv, _) => jv
          }
        )
      }
    }

    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val results = toJson(table.transform {
        Equal(
          DerefObjectStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathField("value1")),
          DerefObjectStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathField("value2"))
        )
      })

      val expected = sample.data flatMap { jv =>
        ((jv \ "value" \ "value1"), (jv \ "value" \ "value2")) match {
          case (JNothing, _) => None
          case (_, JNothing) => None
          case (x, y) => Some(JBool(x == y))
        }
      }

      results.copoint must_== expected
    }
  }
  
  def checkWrapObject = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val results = toJson(table.transform {
        WrapObject(Leaf(Source), "foo")
      })

      val expected = sample.data map { jv => JObject(JField("foo", jv) :: Nil) }
      
      results.copoint must_== expected
    }
  }

  def checkObjectConcatSelf = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val results = toJson(table.transform {
        ObjectConcat(Leaf(Source), Leaf(Source))
      })

      results.copoint must_== sample.data
    }
  }

  def checkObjectConcat = {
    implicit val gen = sample(_ => Seq(JPath("value1") -> CLong, JPath("value2") -> CLong))
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val results = toJson(table.transform {
        ObjectConcat(
          WrapObject(WrapObject(DerefObjectStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathField("value1")), "value1"), "value"), 
          WrapObject(WrapObject(DerefObjectStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathField("value2")), "value2"), "value") 
        )
      })

      results.copoint must_== (sample.data flatMap {
        case JObject(fields) => {
          val back = JObject(fields filter { f => f.name == "value" && f.value.isInstanceOf[JObject] })
          if (back \ "value" \ "value1" == JNothing || back \ "value" \ "value2" == JNothing)
            None
          else
            Some(back)
        }
        
        case _ => None
      })
    }
  }

  def checkObjectConcatOverwrite = {
    implicit val gen = sample(_ => Seq(JPath("value1") -> CLong, JPath("value2") -> CLong))
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val results = toJson(table.transform {
        ObjectConcat(
          WrapObject(DerefObjectStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathField("value1")), "value1"),
          WrapObject(DerefObjectStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathField("value2")), "value1")
        )
      })

      results.copoint must_== (sample.data map { _ \ "value" } collect {
        case v if (v \ "value1") != JNothing && (v \ "value2") != JNothing =>
          JObject(JField("value1", v \ "value2") :: Nil)
      })
    }
  }

  def checkArrayConcat = {
    implicit val gen = sample(_ => Seq(JPath("[0]") -> CLong, JPath("[1]") -> CLong))
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val results = toJson(table.transform {
        WrapObject(
          ArrayConcat(
            WrapArray(DerefArrayStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathIndex(0))),
            WrapArray(DerefArrayStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathIndex(1)))
          ), 
          "value"
        )
      })
      
      results.copoint must_== (sample.data flatMap {
        case obj @ JObject(fields) => {
          (obj \ "value") match {
            case JArray(inner) if inner.length >= 2 =>
              Some(JObject(JField("value", JArray(inner take 2)) :: Nil))
            
            case _ => None
          }
        }
        
        case _ => None
      })
    }
  }

  def checkObjectDelete = {
    implicit val gen = sample(objectSchema(_, 3))

    def randomDeletionMask(schema: CValueGenerators.JSchema): Option[JPathField] = {
      Random.shuffle(schema).headOption.map({ case (JPath(x @ JPathField(_), _ @ _*), _) => x })
    }

    check { (sample: SampleData) =>
      val toDelete = sample.schema.flatMap({ case (_, schema) => randomDeletionMask(schema) })
      toDelete.isDefined ==> {
        val table = fromSample(sample)

        val Some(field) = toDelete

        val result = toJson(table.transform {
          ObjectDelete(DerefObjectStatic(Leaf(Source), JPathField("value")), Set(field)) 
        })

        val expected = sample.data.flatMap { jv => (jv \ "value").delete(JPath(field)) }

        result.copoint must_== expected
      }
    }
  }

  /*
  def checkObjectDelete = {
    implicit val gen = sample(schema)
    def randomDeleteMask(schema: JSchema): Option[JType]  = {
      lazy val buildJType: PartialFunction[(JPath, CType), JType] = {
        case (JPath(JPathField(f), xs @ _*), ctype) => 
          if (Random.nextBoolean) JObjectFixedT(Map(f -> buildJType((JPath(xs: _*), ctype))))
          else JObjectFixedT(Map(f -> JType.JUnfixedT))

        case (JPath(JPathIndex(i), xs @ _*), ctype) => 
          if (Random.nextBoolean) JArrayFixedT(Map(i -> buildJType((JPath(xs: _*), ctype))))
          else JArrayFixedT(Map(i -> JType.JUnfixedT))

        case (JPath.Identity, ctype) => 
          if (Random.nextBoolean) {
            JType.JUnfixedT
          } else {
            ctype match {
              case CString => JTextT
              case CBoolean => JBooleanT
              case CLong | CDouble | CNum => JNumberT
              case CNull => JNullT
              case CEmptyObject => JObjectFixedT(Map())
              case CEmptyArray  => JArrayFixedT(Map())
            }
          }
      }

      val result = Random.shuffle(schema).headOption map buildJType
      println("schema: " + schema)
      println("mask: " + result)
      result
    }

    def mask(jv: JValue, tpe: JType): JValue = {
      ((jv, tpe): @unchecked) match {
        case (JObject(fields), JObjectFixedT(m)) =>
          m.headOption map { 
            case (field, tpe @ JObjectFixedT(_)) =>
              JObject(fields.map {
                case JField(`field`, child) => JField(field, mask(child, tpe))
                case unchanged => unchanged
              })

            case (field, tpe @ JArrayFixedT(_)) => 
              JObject(fields.map {
                case JField(`field`, child) => JField(field, mask(child, tpe))
                case unchanged => unchanged
              })

            case (field, JType.JUnfixedT) => 
              JObject(fields.filter {
                case JField(`field`, _) => false
                case _ => true
              })

            case (field, JType.JPrimitiveUnfixedT) => 
              JObject(fields.filter {
                case JField(`field`, JObject(_) | JArray(_)) => true
                case _ => false
              })

            case (field, JNumberT) => JObject(fields.filter {
              case JField(`field`, JInt(_) | JDouble(_)) => false
              case _ => true 
            })

            case (field, JTextT) => JObject(fields.filter {
              case JField(`field`, JString(_)) => false
              case _ => true 
            })

            case (field, JBooleanT) => JObject(fields.filter {
              case JField(`field`, JBool(_)) => false
              case _ => true 
            })

            case (field, JNullT) => JObject(fields filter {
              case JField(`field`, JNull) => false
              case _ => true 
            })
          } getOrElse {
            JObject(Nil)
          }

        case (JArray(elements), JArrayFixedT(m)) =>
          m.headOption map {
            case (index, tpe @ JObjectFixedT(_)) =>
               JArray(elements.zipWithIndex map {
                case (elem, idx) => if (idx == index) mask(elem, tpe) else elem
              })

            case (index, tpe @ JArrayFixedT(_)) => 
               JArray(elements.zipWithIndex map {
                case (elem, idx) => if (idx == index) mask(elem, tpe) else elem
              })

            case (index, JType.JPrimitiveUnfixedT) => 
              JArray(elements.zipWithIndex map {
                case (v @ JObject(_), _) => v
                case (v @ JArray(_), _) => v
                case (_, `index`) => JNothing
                case (v, _) => v
              })

            case (index, JType.JUnfixedT) => JArray(elements.updated(index, JNothing)) 
            case (index, JNumberT) => JArray(elements.updated(index, JNothing))
            case (index, JTextT) => JArray(elements.updated(index, JNothing))
            case (index, JBooleanT) => JArray(elements.updated(index, JNothing))
            case (index, JNullT) => JArray(elements.updated(index, JNothing))
          } getOrElse {
            if (elements.isEmpty) JNothing else JArray(elements)
          }

        case (JInt(_) | JDouble(_) | JString(_) | JBool(_) | JNull, JType.JPrimitiveUnfixedT) => JNothing
        case (JInt(_) | JDouble(_), JNumberT) => JNothing
        case (JString(_), JTextT) => JNothing
        case (JBool(_), JBooleanT) => JNothing
        case (JNull, JNullT) => JNothing
      }
    }

    check { (sample: SampleData) => 
      val toDelete = sample.schema.flatMap(randomDeleteMask)
      toDelete.isDefined ==> {
        val table = fromSample(sample)

        val Some(jtpe) = toDelete

        val result = toJson(table.transform {
          ObjectDelete(DerefObjectStatic(Leaf(Source), JPathField("value")), jtpe) 
        })

        val expected = sample.data.map { jv => mask(jv \ "value", jtpe).remove(v => v == JNothing || v == JArray(Nil)) } 

        result must_== expected
      }
    }
  }
  */

  def checkTypedTrivial = {
    implicit val gen = sample(_ => Seq(JPath("value1") -> CLong, JPath("value2") -> CBoolean, JPath("value3") -> CLong))
    check { (sample: SampleData) =>
      val table = fromSample(sample)

      val results = toJson(table.transform {
        Typed(Leaf(Source),
          JObjectFixedT(Map("value" ->
            JObjectFixedT(Map("value1" -> JNumberT, "value3" -> JNumberT))))
        )
      })

      val expected = sample.data flatMap { jv =>
        val value1 = jv \ "value" \ "value1"
        val value3 = jv \ "value" \ "value3"
        
        if (value1.isInstanceOf[JNum] && value3.isInstanceOf[JNum]) {
          Some(JObject(
            JField("value",
              JObject(
                JField("value1", jv \ "value" \ "value1") ::
                JField("value3", jv \ "value" \ "value3") ::
                Nil)) ::
            Nil))
        } else {
          None
        }
      }

      results.copoint must_== expected
    }
  }

  def checkTyped = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val schema = sample.schema.getOrElse(0 -> List())._2
      val reducedSchema = schema.zipWithIndex.collect { case (ctpe, i) if i%2 == 0 => ctpe }
      val valuejtpe = Schema.mkType(reducedSchema).getOrElse(JObjectFixedT(Map()))
      val jtpe = JObjectFixedT(Map(
        "value" -> valuejtpe,
        "key" -> JArrayUnfixedT
      ))

      val table = fromSample(sample)
      val results = toJson(table.transform(
        Typed(Leaf(Source), jtpe)
      ))

      val included = reducedSchema.toMap

      val expected = sample.data flatMap { jv =>
        val back = JValue.unflatten(jv.flattenWithPath.filter {
          case (path @ JPath(JPathField("key"), _*), _) => true
          case (path @ JPath(JPathField("value"), tail @ _*), value) if included.contains(JPath(tail : _*)) => {
            (included(JPath(tail : _*)), value) match {
              case (CBoolean, JBool(_)) => true
              case (CString, JString(_)) => true
              case (CLong | CDouble | CNum, JNum(_)) => true
              case (CEmptyObject, JObject.empty) => true
              case (CEmptyArray, JArray.empty) => true
              case (CNull, JNull) => true
              case _ => false
            }
          }
          case _ => false
        })
        
        if (back \ "value" == JNothing)
          None
        else
          Some(back)
      }

      results.copoint must_== expected
    }
  }

  def checkScan = {
    implicit val gen = sample(_ => Seq(JPath.Identity -> CLong))
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val results = toJson(table.transform {
        Scan(DerefObjectStatic(Leaf(Source), JPathField("value")), lookupScanner(Nil, "sum"))
      })

      val (_, expected) = sample.data.foldLeft((BigDecimal(0), Vector.empty[JValue])) { 
        case ((a, s), jv) => { 
          (jv \ "value") match {
            case JNum(i) => (a + i, s :+ JNum(a + i))
            case _ => (a, s)
          }
        }
      }

      results.copoint must_== expected.toStream
    }
  }

  def testDerefObjectDynamic = {
    val data =  JObject(JField("foo", JNum(1)) :: JField("ref", JString("foo")) :: Nil) #::
                JObject(JField("bar", JNum(2)) :: JField("ref", JString("bar")) :: Nil) #::
                JObject(JField("baz", JNum(3)) :: JField("ref", JString("baz")) :: Nil) #:: Stream.empty[JValue]

    val table = fromSample(SampleData(data))
    val results = toJson(table.transform {
      DerefObjectDynamic(
        Leaf(Source),
        DerefObjectStatic(Leaf(Source), JPathField("ref"))
      )
    })

    val expected = JNum(1) #:: JNum(2) #:: JNum(3) #:: Stream.empty[JValue]

    results.copoint must_== expected
  }

  def checkArraySwap = {
    implicit val gen = sample(arraySchema(_, 3))
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val results = toJson(table.transform {
        ArraySwap(DerefObjectStatic(Leaf(Source), JPathField("value")), 2)
      })

      val expected = sample.data flatMap { jv =>
        (jv \ "value") match {
          case JArray(x :: y :: z :: xs) => Some(JArray(z :: y :: x :: xs))
          case _ => None
        }
      }

      results.copoint must_== expected
    }
  }

  def checkConst = {
    implicit val gen = undefineRowsForColumn(sample(_ => Seq(JPath("field") -> CLong)), JPath("value") \ "field")
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val results = toJson(table.transform(ConstLiteral(CString("foo"), DerefObjectStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathField("field")))))
      
      val expected = sample.data flatMap {
        case jv if jv \ "value" \ "field" == JNothing => None
        case _ => Some(JString("foo"))
      }

      results.copoint must_== expected
    }
  }
}

// vim: set ts=4 sw=4 et:
