package com.precog.yggdrasil

import blueeyes.json.{ JPath, JPathField, JPathIndex }
import blueeyes.json.JsonAST._

import org.specs2.mutable._

import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

import com.precog.bytecode._

trait TransformSpec extends TableModuleSpec {
  import trans._

  def checkTransformLeaf = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val table = fromJson(sample)
      val results = toJson(table.transform(Leaf(Source)))

      results must_== sample.data
    }
  }

  def testMap1IntLeaf = {
    val sample = (-10 to 10).map(JInt(_)).toStream
    val table = fromJson(SampleData(sample))
    val results = toJson(table.transform { Map1(Leaf(Source), lookupF1(Nil, "negate")) })

    results must_== (-10 to 10).map(x => JInt(-x))
  }

  /* Do we want to allow non-boolean sets to be used as filters without an explicit existence predicate?
  def checkTrivialFilter = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val table = fromJson(sample)
      val results = toJson(table.transform {
        Filter(
          Leaf(Source), 
          Leaf(Source)
        )
      })

      results must_== sample.data
    }
  }
  */

  def checkTrueFilter = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val table = fromJson(sample)
      val results = toJson(table.transform {
        Filter(
          Leaf(Source), 
          Map1(Leaf(Source), lookupF1(Nil, "true"))
        )
      })

      results must_== sample.data
    }
  }

  def checkFilter = {
    implicit val gen = sample(_ => Gen.value(Seq(JPath.Identity -> CLong)))
    check { (sample: SampleData) =>
      val table = fromJson(sample)
      val results = toJson(table.transform {
        Filter(
          Leaf(Source), 
          Map1(
            DerefObjectStatic(Leaf(Source), JPathField("value")), 
            lookupF2(Nil, "mod").applyr(CLong(2)) andThen lookupF2(Nil, "eq").applyr(CLong(0))
          )
        )
      })

      val expected = sample.data map { jv =>
        (jv \ "value") match { 
          case JInt(x) if x.longValue % 2 == 0 => jv
          case _ => JNothing 
        }
      }

      results must_== expected
    }
  }

  def checkObjectDeref = {
    implicit val gen = sample(objectSchema(_, 3))
    check { (sample: SampleData) =>
      val (field, _) = sample.schema.get.head
      val fieldHead = field.head.get
      val table = fromJson(sample)
      val results = toJson(table.transform {
        DerefObjectStatic(Leaf(Source), fieldHead.asInstanceOf[JPathField])
      })

      val expected = sample.data.map { jv => jv(JPath(fieldHead)) }

      results must_== expected
    }
  }

  def checkArrayDeref = {
    implicit val gen = sample(arraySchema(_, 3))
    check { (sample: SampleData) =>
      val (field, _) = sample.schema.get.head
      val fieldHead = field.head.get
      val table = fromJson(sample)
      val results = toJson(table.transform {
        DerefArrayStatic(Leaf(Source), fieldHead.asInstanceOf[JPathIndex])
      })

      val expected = sample.data.map { jv => jv(JPath(fieldHead)) }

      results must_== expected
    }
  }

  def checkMap2 = {
    implicit val gen = sample(_ => Seq(JPath("value1") -> CLong, JPath("value2") -> CLong))
    check { (sample: SampleData) =>
      val table = fromJson(sample)
      val results = toJson(table.transform {
        Map2(
          DerefObjectStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathField("value1")),
          DerefObjectStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathField("value2")),
          lookupF2(Nil, "add")
        )
      })

      val expected = sample.data map { jv =>
        ((jv \ "value" \ "value1"), (jv \ "value" \ "value2")) match {
          case (JInt(x), JInt(y)) => JInt(x+y)
          case _ => failure("Bogus test data")
        }
      }

      results must_== expected
    }
  }

  def checkEqualSelf = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val table = fromJson(sample)
      val results = toJson(table.transform {
        Equal(Leaf(Source), Leaf(Source))
      })

      results must_== (Stream.tabulate(sample.data.size) { _ => JBool(true) })
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
      val table = fromJson(sample)
      val results = toJson(table.transform {
        Equal(
          DerefObjectStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathField("value1")),
          DerefObjectStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathField("value2"))
        )
      })

      val expected = sample.data.map { jv =>
        ((jv \ "value" \ "value1"), (jv \ "value" \ "value2")) match {
          case (JInt(x), JInt(y))  => JBool(x == y)
          case (JNothing, JInt(y)) => JNothing
          case (JInt(x), JNothing) => JNothing
          case _ => failure("Bogus test data")
        }
      }

      results must_== expected
    }
  }

  def checkWrapObject = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val table = fromJson(sample)
      val results = toJson(table.transform {
        WrapObject(Leaf(Source), "foo")
      })

      val expected = sample.data map { jv => JObject(JField("foo", jv) :: Nil) }
      
      results must_== expected
    }
  }

  def checkObjectConcatSelf = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val table = fromJson(sample)
      val results = toJson(table.transform {
        ObjectConcat(Leaf(Source), Leaf(Source))
      })

      results must_== sample.data
    }
  }

  def checkObjectConcat = {
    implicit val gen = sample(_ => Seq(JPath("value1") -> CLong, JPath("value2") -> CLong))
    check { (sample: SampleData) =>
      val table = fromJson(sample)
      val results = toJson(table.transform {
        ObjectConcat(
          WrapObject(WrapObject(DerefObjectStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathField("value1")), "value1"), "value"), 
          WrapObject(WrapObject(DerefObjectStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathField("value2")), "value2"), "value") 
        )
      })

      results must_== sample.data.map({ case JObject(fields) => JObject(fields.filter(_.name == "value")) })
    }
  }

  def checkObjectConcatOverwrite = {
    implicit val gen = sample(_ => Seq(JPath("value1") -> CLong, JPath("value2") -> CLong))
    check { (sample: SampleData) =>
      val table = fromJson(sample)
      val results = toJson(table.transform {
        ObjectConcat(
          WrapObject(DerefObjectStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathField("value1")), "value1"),
          WrapObject(DerefObjectStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathField("value2")), "value1")
        )
      })

      results must_== (sample.data map { _ \ "value" } map { case v => JObject(JField("value1", v \ "value2") :: Nil) })
    }
  }

  def checkArrayConcat = {
    implicit val gen = sample(_ => Seq(JPath("[0]") -> CLong, JPath("[1]") -> CLong))
    check { (sample: SampleData) =>
      val table = fromJson(sample)
      val results = toJson(table.transform {
        WrapObject(
          ArrayConcat(
            WrapArray(DerefArrayStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathIndex(0))),
            WrapArray(DerefArrayStatic(DerefObjectStatic(Leaf(Source), JPathField("value")), JPathIndex(1)))
          ), 
          "value"
        )
      })

      results must_== sample.data.map({ case JObject(fields) => JObject(fields.filter(_.name == "value")) })
    }
  }

  def checkTypedTrivial = {
    implicit val gen = sample(_ => Seq(JPath("value1") -> CLong, JPath("value2") -> CBoolean, JPath("value3") -> CLong))
    check { (sample: SampleData) =>
      val table = fromJson(sample)

      val results = toJson(table.transform {
        Typed(Leaf(Source),
          JObjectFixedT(Map("value" ->
            JObjectFixedT(Map("value1" -> JNumberT, "value3" -> JNumberT))))
        )
      })

      val expected = sample.data map { jv =>
        JObject(
          JField("value",
            JObject(
              JField("value1", jv \ "value" \ "value1") ::
              JField("value3", jv \ "value" \ "value3") ::
              Nil)) ::
          Nil)
      }

      results must_== expected
    }
  }

  def checkTyped = {
    implicit val gen = sample(schema)
    check { (sample: SampleData) =>
      val schema = sample.schema.getOrElse(List())
      val reducedSchema = schema.zipWithIndex.collect { case (ctpe, i) if i%2 == 0 => ctpe }
      val valuejtpe = Schema.mkType(reducedSchema).getOrElse(JObjectFixedT(Map()))
      val jtpe = JObjectFixedT(Map(
        "value" -> valuejtpe,
        "key" -> JArrayUnfixedT
      ))

      val table = fromJson(sample)
      val results = toJson(table.transform(
        Typed(Leaf(Source), jtpe)
      ))

      val included = reducedSchema.toMap

      val expected = sample.data map { jv =>
        JValue.unflatten(jv.flattenWithPath.filter {
          case (path @ JPath(JPathField("key"), _*), _) => true
          case (path @ JPath(JPathField("value"), tail @ _*), value) if included.contains(JPath(tail : _*)) => {
            (included(JPath(tail : _*)), value) match {
              case (CBoolean, JBool(_)) => true
              case (CStringFixed(_) | CStringArbitrary, JString(_)) => true
              case (CLong, JInt(_)) => true
              case (CDouble | CDecimalArbitrary, JDouble(_)) => true
              case (CEmptyObject, JObject.empty) => true
              case (CEmptyArray, JArray.empty) => true
              case (CNull, JNull) => true
              case _ => false
            }
          }
          case _ => false
        })
      }

      results must_== expected
    }
  }

  def checkScan = {
    implicit val gen = sample(_ => Seq(JPath.Identity -> CLong))
    check { (sample: SampleData) =>
      val table = fromJson(sample)
      val results = toJson(table.transform {
        Scan(DerefObjectStatic(Leaf(Source), JPathField("value")), lookupScanner(Nil, "sum"))
      })

      val (_, expected) = sample.data.foldLeft((BigInt(0), Vector.empty[JValue])) { 
        case ((a, s), jv) => 
          val JInt(i) = jv \ "value"
          (a + i, s :+ JDouble((a + i).toDouble))
      }

      results must_== expected.toStream
    }
  }

  def testDerefObjectDynamic = {
    val data =  JObject(JField("foo", JInt(1)) :: JField("ref", JString("foo")) :: Nil) #::
                JObject(JField("bar", JInt(2)) :: JField("ref", JString("bar")) :: Nil) #::
                JObject(JField("baz", JInt(3)) :: JField("ref", JString("baz")) :: Nil) #:: Stream.empty[JValue]

    val table = fromJson(SampleData(data))
    val results = toJson(table.transform {
      DerefObjectDynamic(
        Leaf(Source),
        DerefObjectStatic(Leaf(Source), JPathField("ref"))
      )
    })

    val expected = JInt(1) #:: JInt(2) #:: JInt(3) #:: Stream.empty[JValue]

    results must_== expected
  }

  def checkArraySwap = {
    implicit val gen = sample(arraySchema(_, 3))
    check { (sample: SampleData) =>
      val table = fromJson(sample)
      val results = toJson(table.transform {
        ArraySwap(DerefObjectStatic(Leaf(Source), JPathField("value")), 2)
      })

      val expected = sample.data map { jv =>
        ((jv \ "value"): @unchecked) match {
          case JArray(x :: y :: z :: xs) => JArray(z :: y :: x :: xs)
        }
      }

      results must_== expected
    }
  }
}

// vim: set ts=4 sw=4 et:
