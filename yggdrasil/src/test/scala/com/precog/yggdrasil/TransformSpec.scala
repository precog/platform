package com.precog.yggdrasil

import com.precog.common.json._

import akka.dispatch.Await
import akka.util.Duration

import blueeyes.json._
import blueeyes.json.JsonAST._

import org.specs2.ScalaCheck
import org.specs2.mutable._

import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

import com.precog.bytecode._
import scala.util.Random
import scalaz.syntax.copointed._

trait TransformSpec[M[+_]] extends TableModuleTestSupport[M] with Specification with ScalaCheck {
  import CValueGenerators._
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
    implicit val gen = sample(_ => Gen.value(Seq(CPath.Identity -> CLong)))
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val results = toJson(table.transform {
        Filter(
          Leaf(Source), 
          Map1(
            DerefObjectStatic(Leaf(Source), CPathField("value")), 
            lookupF2(Nil, "mod").applyr(CLong(2)) andThen lookupF2(Nil, "eq").applyr(CLong(0))
          )
        )
      })

      val expected = sample.data flatMap { jv =>
        (jv \ "value") match { 
          case JNum(x) if x % 2 == 0 => Some(jv)
          case _ => None
        }
      }

      results.copoint must_== expected
    }.set(minTestsOk -> 200)
  }

  def checkObjectDeref = {
    implicit val gen = sample(objectSchema(_, 3))
    check { (sample: SampleData) =>
      val (field, _) = sample.schema.get._2.head
      val fieldHead = field.head.get
      val table = fromSample(sample)
      val results = toJson(table.transform {
        DerefObjectStatic(Leaf(Source), CPathField(fieldHead.asInstanceOf[CPathField].name))
      })

      val expected = sample.data.map { jv => jv(CPath(fieldHead)) } flatMap {
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
        DerefArrayStatic(Leaf(Source), CPathIndex(fieldHead.asInstanceOf[CPathIndex].index))
      })

      val expected = sample.data.map { jv => jv(CPath(fieldHead)) } flatMap {
        case JNothing => None
        case jv       => Some(jv)
      }

      results.copoint must_== expected
    }
  }

  def checkMap2 = {
    implicit val gen = sample(_ => Seq(CPath("value1") -> CLong, CPath("value2") -> CLong))
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val results = toJson(table.transform {
        Map2(
          DerefObjectStatic(DerefObjectStatic(Leaf(Source), CPathField("value")), CPathField("value1")),
          DerefObjectStatic(DerefObjectStatic(Leaf(Source), CPathField("value")), CPathField("value2")),
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

  def checkEqualSelfArray = {
    val array: JValue = JsonParser.parse("""
      [[9,10,11]]""")

    val data: Stream[JValue] = (array match {
      case JArray(li) => li
      case _ => sys.error("expected JArray")
    }).map { k => { JObject(List(JField("value", k), JField("key", JArray(List(JNum(0)))))) } }.toStream
    
    println("data: %s\n".format(data.toList))

    val sample = SampleData(data)
    val table = fromSample(sample)

    val data2: Stream[JValue] = Stream(
      JObject(List(JField("value", JArray(List(JNum(9), JNum(10), JNum(11)))), JField("key", JArray(List())))))

    val sample2 = SampleData(data2)
    val table2 = fromSample(sample2)
    
    val leftIdentitySpec = DerefObjectStatic(Leaf(SourceLeft), CPathField("key"))
    val rightIdentitySpec = DerefObjectStatic(Leaf(SourceRight), CPathField("key"))
    
    val newIdentitySpec = ArrayConcat(leftIdentitySpec, rightIdentitySpec)
    
    val wrappedIdentitySpec = trans.WrapObject(newIdentitySpec, "key")

    val leftValueSpec = DerefObjectStatic(Leaf(SourceLeft), CPathField("value"))
    val rightValueSpec = DerefObjectStatic(Leaf(SourceRight), CPathField("value"))
    
    val wrappedValueSpec = trans.WrapObject(Equal(leftValueSpec, rightValueSpec), "value")

    val results = toJson(table.cross(table2)(InnerObjectConcat(wrappedIdentitySpec, wrappedValueSpec)))
    val expected = (data map {
      case jo @ JObject(List(JField("value", v), key @ _)) => {
        if (v == JArray(List(JNum(9), JNum(10), JNum(11)))) 
          JObject(List(JField("value", JBool(true)), key))
        else JObject(List(JField("value", JBool(false)), key))
      }
      case _ => sys.error("unreachable case")
    }).toStream

    results.copoint must_== expected
  }

  def checkEqual = {
    val genBase: Gen[SampleData] = sample(_ => Seq(CPath("value1") -> CLong, CPath("value2") -> CLong)).arbitrary
    implicit val gen: Arbitrary[SampleData] = Arbitrary {
      genBase map { sd =>
        SampleData(
          sd.data.zipWithIndex map {
            case (jv, i) if i%2 == 0 => 
              // construct object with value1 == value2
              jv.set(CPath("value/value2"), jv(CPath("value/value1")))

            case (jv, i) if i%5 == 0 => // delete value1
              jv.set(CPath("value/value1"), JNothing)

            case (jv, i) if i%5 == 3 => // delete value2
              jv.set(CPath("value/value2"), JNothing)

            case (jv, _) => jv
          }
        )
      }
    }

    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val results = toJson(table.transform {
        Equal(
          DerefObjectStatic(DerefObjectStatic(Leaf(Source), CPathField("value")), CPathField("value1")),
          DerefObjectStatic(DerefObjectStatic(Leaf(Source), CPathField("value")), CPathField("value2"))
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
        InnerObjectConcat(Leaf(Source), Leaf(Source))
      })

      results.copoint must_== sample.data
    }
  }

  def testObjectConcatSingletonNonObject = {
    val data: Stream[JValue] = Stream(JBool(true))
    val sample = SampleData(data)
    val table = fromSample(sample)

    val results = toJson(table.transform {
      InnerObjectConcat(Leaf(Source))
    })

    results.copoint must beEmpty
  }

  def testObjectConcatTrivial = {
    val data: Stream[JValue] = Stream(JBool(true), JObject(List()))
    val sample = SampleData(data)
    val table = fromSample(sample)

    val results = toJson(table.transform {
      InnerObjectConcat(Leaf(Source))
    })

    results.copoint must beEmpty
  }

  def checkObjectConcat = {
    implicit val gen = sample(_ => Seq(CPath("value1") -> CLong, CPath("value2") -> CLong))
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val results = toJson(table.transform {
        InnerObjectConcat(
          WrapObject(WrapObject(DerefObjectStatic(DerefObjectStatic(Leaf(Source), CPathField("value")), CPathField("value1")), "value1"), "value"), 
          WrapObject(WrapObject(DerefObjectStatic(DerefObjectStatic(Leaf(Source), CPathField("value")), CPathField("value2")), "value2"), "value") 
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
    implicit val gen = sample(_ => Seq(CPath("value1") -> CLong, CPath("value2") -> CLong))
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val results = toJson(table.transform {
        InnerObjectConcat(
          WrapObject(DerefObjectStatic(DerefObjectStatic(Leaf(Source), CPathField("value")), CPathField("value1")), "value1"),
          WrapObject(DerefObjectStatic(DerefObjectStatic(Leaf(Source), CPathField("value")), CPathField("value2")), "value1")
        )
      })

      results.copoint must_== (sample.data map { _ \ "value" } collect {
        case v if (v \ "value1") != JNothing && (v \ "value2") != JNothing =>
          JObject(JField("value1", v \ "value2") :: Nil)
      })
    }
  }

  def checkArrayConcat = {
    implicit val gen = sample(_ => Seq(CPath("[0]") -> CLong, CPath("[1]") -> CLong))
    check { (sample0: SampleData) =>
      /***
      important note:
      `sample` is excluding the cases when we have JArrays of size 1
      this is because then the concat array would return
      array.concat(undefined) = array
      which is incorrect but is what the code currently does
      */
      
      val sample = SampleData(sample0.data flatMap { jv =>
        (jv \ "value") match {
          case JArray(x :: Nil) => None
          case z => Some(z)
        }
      })

      val table = fromSample(sample)
      val results = toJson(table.transform {
        WrapObject(
          ArrayConcat(
            WrapArray(DerefArrayStatic(DerefObjectStatic(Leaf(Source), CPathField("value")), CPathIndex(0))),
            WrapArray(DerefArrayStatic(DerefObjectStatic(Leaf(Source), CPathField("value")), CPathIndex(1)))
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

    def randomDeletionMask(schema: CValueGenerators.JSchema): Option[CPathField] = {
      Random.shuffle(schema).headOption.map({ case (CPath(x @ CPathField(_), _ @ _*), _) => x })
    }

    check { (sample: SampleData) =>
      val toDelete = sample.schema.flatMap({ case (_, schema) => randomDeletionMask(schema) })
      toDelete.isDefined ==> {
        val table = fromSample(sample)

        val Some(field) = toDelete

        val result = toJson(table.transform {
          ObjectDelete(DerefObjectStatic(Leaf(Source), CPathField("value")), Set(CPathField(field.name)))
        })

        val expected = sample.data.flatMap { jv => (jv \ "value").delete(CPath(field)) }

        result.copoint must_== expected
      }
    }
  }

  /*
  def checkObjectDelete = {
    implicit val gen = sample(schema)
    def randomDeleteMask(schema: JSchema): Option[JType]  = {
      lazy val buildJType: PartialFunction[(CPath, CType), JType] = {
        case (CPath(CPathField(f), xs @ _*), ctype) => 
          if (Random.nextBoolean) JObjectFixedT(Map(f -> buildJType((CPath(xs: _*), ctype))))
          else JObjectFixedT(Map(f -> JType.JUnfixedT))

        case (CPath(CPathIndex(i), xs @ _*), ctype) => 
          if (Random.nextBoolean) JArrayFixedT(Map(i -> buildJType((CPath(xs: _*), ctype))))
          else JArrayFixedT(Map(i -> JType.JUnfixedT))

        case (CPath.Identity, ctype) => 
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
          ObjectDelete(DerefObjectStatic(Leaf(Source), CPathField("value")), jtpe) 
        })

        val expected = sample.data.map { jv => mask(jv \ "value", jtpe).remove(v => v == JNothing || v == JArray(Nil)) } 

        result must_== expected
      }
    }
  }
  */

  def checkTypedTrivial = {
    implicit val gen = sample(_ => Seq(CPath("value1") -> CLong, CPath("value2") -> CBoolean, CPath("value3") -> CLong))
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

  def checkTypedHeterogeneous = {
    val data: Stream[JValue] = 
      Stream(
        JObject(List(JField("value", JString("value1")), JField("key", JArray(List(JNum(1)))))), 
        JObject(List(JField("value", JNum(23)), JField("key", JArray(List(JNum(2)))))))
    val sample = SampleData(data)
    val table = fromSample(sample)

    val jtpe = JObjectFixedT(Map("value" -> JTextT, "key" -> JArrayUnfixedT))
    val results = toJson(table.transform {
      Typed(Leaf(Source), jtpe)
    })
    
    val included: Map[CPath, CType] = Map(CPath(List()) -> CString)

    val sampleSchema = inferSchema(data.toSeq)
    val subsumes: Boolean = Schema.subsumes(sampleSchema, jtpe)

    results.copoint must_== expectedResult(data, included, subsumes)
  }

  def checkTypedObject = {
    val data: Stream[JValue] = 
      Stream(
        JObject(List(JField("value", JObject(List(JField("foo", JNum(23))))), JField("key", JArray(List(JNum(1), JNum(3)))))),
        JObject(List(JField("value", JObject(Nil)), JField("key", JArray(List(JNum(2), JNum(4)))))))
    val sample = SampleData(data)
    val table = fromSample(sample)

    val results = toJson(table.transform {
      Typed(Leaf(Source), 
        JObjectFixedT(Map("value" -> JObjectFixedT(Map("foo" -> JNumberT)), "key" -> JArrayUnfixedT)))
    })

    val expected = Stream(JObject(List(JField("value", JObject(List(JField("foo", JNum(23))))), JField("key", JArray(List(JNum(1), JNum(3)))))))

    results.copoint must_== expected
  } 

  def checkTypedArray = {
    val data: Stream[JValue] = 
      Stream(
        JObject(List(JField("value", JArray(List(JNum(2), JBool(true)))), JField("key", JArray(List(JNum(1), JNum(2)))))),
        JObject(List(JField("value", JObject(List())), JField("key", JArray(List(JNum(3), JNum(4)))))))
    val sample = SampleData(data)
    val table = fromSample(sample)

    val results = toJson(table.transform {
      Typed(Leaf(Source), 
        JObjectFixedT(Map("value" -> JArrayFixedT(Map(0 -> JNumberT, 1 -> JBooleanT)), "key" -> JArrayUnfixedT)))
    })

    val expected = Stream(JObject(List(JField("value", JArray(List(JNum(2), JBool(true)))), JField("key", JArray(List(JNum(1), JNum(2)))))))

    val resultStream = results.copoint
    resultStream must haveSize(1)
    resultStream must_== expected
  } 

  def checkTypedArray2 = {
    val data: Stream[JValue] = 
      Stream(
        JObject(List(JField("value", JArray(List(JNum(2), JBool(true)))), JField("key", JArray(List(JNum(1)))))))
    val sample = SampleData(data)
    val table = fromSample(sample)
    val jtpe = JObjectFixedT(Map("value" -> JArrayFixedT(Map(1 -> JBooleanT)), "key" -> JArrayUnfixedT))

    val results = toJson(table.transform {
      Typed(Leaf(Source), jtpe)
    })
   
    val included: Map[CPath, CType] = Map(CPath(List(CPathIndex(1))) -> CBoolean)

    val sampleSchema = inferSchema(data.toSeq)
    val subsumes: Boolean = Schema.subsumes(sampleSchema, jtpe)

    results.copoint must_== expectedResult(data, included, subsumes)
  }

  def checkTypedArray4 = {
    val data: Stream[JValue] = 
      Stream(
        JObject(List(JField("value", JArray(List(JNum(2.4), JNum(12), JBool(true), JArray(List())))), JField("key", JArray(List(JNum(1)))))),
        JObject(List(JField("value", JArray(List(JNum(3.5), JNull, JBool(false)))), JField("key", JArray(List(JNum(2)))))))
    val sample = SampleData(data)
    val table = fromSample(sample)

    val jtpe = JObjectFixedT(Map("value" -> JArrayFixedT(Map(0 -> JNumberT, 1 -> JNumberT, 2 -> JBooleanT, 3 -> JArrayFixedT(Map()))), "key" -> JArrayUnfixedT))
    val results = toJson(table.transform {
      Typed(Leaf(Source), jtpe)
    })
    
    val included: Map[CPath, CType] = 
      Map(CPath(List(CPathIndex(0))) -> CNum, CPath(List(CPathIndex(1))) -> CNum, CPath(List(CPathIndex(2))) -> CBoolean, CPath(List(CPathIndex(3))) -> CEmptyArray)

    val sampleSchema = inferSchema(data.toSeq)
    val subsumes: Boolean = Schema.subsumes(sampleSchema, jtpe)

    results.copoint must_== expectedResult(data, included, subsumes)
  }

  def checkTypedArray3 = {
    val data: Stream[JValue] = 
      Stream(
        JObject(List(JField("value", JArray(List(JArray(List()), JNum(23), JNull))), JField("key", JArray(List(JNum(1)))))),
        JObject(List(JField("value", JArray(List(JArray(List()), JArray(List()), JNull))), JField("key", JArray(List(JNum(2)))))))
    val sample = SampleData(data)
    val table = fromSample(sample)

    val jtpe = JObjectFixedT(Map("value" -> JArrayFixedT(Map(0 -> JArrayFixedT(Map()), 1 -> JArrayFixedT(Map()), 2 -> JNullT)), "key" -> JArrayUnfixedT))

    val results = toJson(table.transform {
      Typed(Leaf(Source), jtpe)
    })
      
    val included: Map[CPath, CType] = Map(CPath(List(CPathIndex(0))) -> CEmptyArray, CPath(List(CPathIndex(1))) -> CEmptyArray, CPath(List(CPathIndex(2))) -> CNull)

    val sampleSchema = inferSchema(data.toSeq)
    val subsumes: Boolean = Schema.subsumes(sampleSchema, jtpe)

    results.copoint must_== expectedResult(data, included, subsumes)
  }

  def checkTypedObject2 = {
    val data: Stream[JValue] = 
      Stream(
        JObject(List(JField("value", JObject(List(JField("foo", JBool(true)), JField("bar", JNum(77))))), JField("key", JArray(List(JNum(1)))))))
    val sample = SampleData(data)
    val table = fromSample(sample)

    val results = toJson(table.transform {
      Typed(Leaf(Source), 
        JObjectFixedT(Map("value" -> JObjectFixedT(Map("bar" -> JNumberT)), "key" -> JArrayUnfixedT)))
    })

    val expected = Stream(JObject(List(JField("value", JObject(List(JField("bar", JNum(77))))), JField("key", JArray(List(JNum(1)))))))
    results.copoint must_== expected
  }  
  
  def checkTypedNumber = {
    val data: Stream[JValue] = 
      Stream(
        JObject(List(JField("value", JNum(23)), JField("key", JArray(List(JNum(1), JNum(3)))))),
        JObject(List(JField("value", JString("foo")), JField("key", JArray(List(JNum(2), JNum(4)))))))
    val sample = SampleData(data)
    val table = fromSample(sample)

    val results = toJson(table.transform {
      Typed(Leaf(Source), 
        JObjectFixedT(Map("value" -> JNumberT, "key" -> JArrayUnfixedT)))
    })

    val expected = Stream(JObject(List(JField("value", JNum(23)), JField("key", JArray(List(JNum(1), JNum(3)))))))

    results.copoint must_== expected
  }  

  def checkTypedNumber2 = {
    val data: Stream[JValue] = 
      Stream(
        JObject(List(JField("value", JNum(23)), JField("key", JArray(List(JNum(1), JNum(3)))))),
        JObject(List(JField("value", JNum(12.5)), JField("key", JArray(List(JNum(2), JNum(4)))))))
    val sample = SampleData(data)
    val table = fromSample(sample)

    val results = toJson(table.transform {
      Typed(Leaf(Source), 
        JObjectFixedT(Map("value" -> JNumberT, "key" -> JArrayUnfixedT)))
    })

    val expected = data

    results.copoint must_== expected
  }

  def checkTypedEmpty = {
    val data: Stream[JValue] = 
      Stream(
        JObject(List(JField("value", JField("foo", JArray(List()))), JField("key", JArray(List(JNum(1)))))))
    val sample = SampleData(data)
    val table = fromSample(sample)

    val results = toJson(table.transform {
      Typed(Leaf(Source), 
        JObjectFixedT(Map("value" -> JArrayFixedT(Map()), "key" -> JArrayUnfixedT)))
    })

    results.copoint must beEmpty
  }

  def checkTyped = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val schema = sample.schema.getOrElse(0 -> List())._2
      //val reducedSchema = schema.zipWithIndex.collect { case (ctpe, i) if i%2 == 0 => ctpe }
      val valuejtpe = Schema.mkType(schema).getOrElse(JObjectFixedT(Map()))  //reducedSchema
      val jtpe = JObjectFixedT(Map(
        "value" -> valuejtpe,
        "key" -> JArrayUnfixedT
      ))

      val table = fromSample(sample)
      val results = toJson(table.transform(
        Typed(Leaf(Source), jtpe)))

      val included = schema.toMap  //reducedSchema

      val sampleSchema = inferSchema(sample.data.toSeq)
      val subsumes: Boolean = Schema.subsumes(sampleSchema, jtpe)

      results.copoint must_== expectedResult(sample.data, included, subsumes)
    }.set(minTestsOk -> 10000)
  }
  
  def testTrivialScan = {
    val data = JObject(JField("value", JNum(BigDecimal("2705009941739170689"))) :: JField("key", JArray(JNum(1) :: Nil)) :: Nil) #::
               JObject(JField("value", JString("")) :: JField("key", JArray(JNum(2) :: Nil)) :: Nil) #::
               Stream.empty
               
    val sample = SampleData(data)
    val table = fromSample(sample)
    val results = toJson(table.transform {
      Scan(DerefObjectStatic(Leaf(Source), CPathField("value")), lookupScanner(Nil, "sum"))
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

  def testHetScan = {
    val data = JObject(JField("value", JNum(12)) :: JField("key", JArray(JNum(1) :: Nil)) :: Nil) #::
               JObject(JField("value", JNum(10)) :: JField("key", JArray(JNum(2) :: Nil)) :: Nil) #::
               JObject(JField("value", JArray(JNum(13) :: Nil)) :: JField("key", JArray(JNum(3) :: Nil)) :: Nil) #::
               Stream.empty

    val sample = SampleData(data)
    val table = fromSample(sample)
    val results = toJson(table.transform {
      Scan(DerefObjectStatic(Leaf(Source), CPathField("value")), lookupScanner(Nil, "sum"))
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

  def checkScan = {
    implicit val gen = sample(_ => Seq(CPath.Identity -> CLong))
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val results = toJson(table.transform {
        Scan(DerefObjectStatic(Leaf(Source), CPathField("value")), lookupScanner(Nil, "sum"))
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
        DerefObjectStatic(Leaf(Source), CPathField("ref"))
      )
    })

    val expected = JNum(1) #:: JNum(2) #:: JNum(3) #:: Stream.empty[JValue]

    results.copoint must_== expected
  }

  def checkArraySwap = {
    implicit val gen = sample(arraySchema(_, 3))
    check { (sample0: SampleData) =>
      /***
      important note:
      `sample` is excluding the cases when we have JArrays of sizes 1 and 2 
      this is because then the array swap would go out of bounds of the index
      and insert an undefined in to the array
      this will never happen in the real system
      so the test ignores this case
      */
      val sample = SampleData(sample0.data flatMap { jv => 
        (jv \ "value") match {
          case JArray(x :: Nil) => None
          case JArray(x :: y :: Nil) => None
          case z => Some(z)
        }
      })
      val table = fromSample(sample)
      val results = toJson(table.transform {
        ArraySwap(DerefObjectStatic(Leaf(Source), CPathField("value")), 2)
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
    implicit val gen = undefineRowsForColumn(sample(_ => Seq(CPath("field") -> CLong)), CPath("value") \ "field")
    check { (sample: SampleData) =>
      val table = fromSample(sample)
      val results = toJson(table.transform(ConstLiteral(CString("foo"), DerefObjectStatic(DerefObjectStatic(Leaf(Source), CPathField("value")), CPathField("field")))))
      
      val expected = sample.data flatMap {
        case jv if jv \ "value" \ "field" == JNothing => None
        case _ => Some(JString("foo"))
      }

      results.copoint must_== expected
    }
  }

  def expectedResult(data: Stream[JValue], included: Map[CPath, CType], subsumes: Boolean): Stream[JValue] = {
    if (subsumes) { 
      data flatMap { jv =>
        val paths = jv.flattenWithPath.toMap.keys.toList

        val includes: Boolean = included.keys forall {
          case CPath(tail) => paths.contains(CPath(CPathField("value"), tail)) 
          case _ => true
        } 

        val filtered = jv.flattenWithPath filter {
          case (CPath(CPathField("value"), tail @ _*), _) if included.contains(CPath(tail: _*)) => true
          case (CPath(CPathField("key"), _*), _) => true
          case _ => false
        }

        lazy val back = JValue.unflatten(
          if (filtered forall {
            case (path @ CPath(CPathField("key"), _*), _) => true
            case (path @ CPath(CPathField("value"), tail @ _*), value) => {
              val (inc, vau) = (included(CPath(tail : _*)), value) 
              (inc, vau) match {
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
          }) filtered else List())

        if (includes) 
          if (back \ "value" == JNothing)
            None
          else
            Some(back)
        else None
      }
    } else {
      Stream()
    }
  }
}

// vim: set ts=4 sw=4 et:
