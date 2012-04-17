package com.precog.common.util

import blueeyes.json.JsonAST
import org.scalacheck._
import Gen._
import Arbitrary.arbitrary

trait ArbitraryJValue {
  import JsonAST._

  def genJValue:  Gen[JValue]  = frequency((10, genSimple), (1, wrap(choose(0, 5) flatMap genArray)), (1, wrap(choose(0, 5) flatMap genObject)))
  def genJInt:    Gen[JInt]    = arbitrary[Int].map(JInt(_))
  def genJDouble: Gen[JDouble] = arbitrary[Double].map(JDouble(_))
  def genJBool:   Gen[JBool]   = arbitrary[Boolean].map(JBool(_))
  def genJString: Gen[JString] = alphaStr.map(JString(_))
  def genSimple: Gen[JValue] = oneOf(
    value(JNull),
    genJInt,
    genJDouble,
    genJBool,
    genJString)
    
  def genSimpleNotNull: Gen[JValue] = oneOf(
    genJInt,
    genJDouble,
    genJBool,
    genJString)

  def genArray(listSize: Int): Gen[JValue] = for (l <- genList(listSize)) yield JArray(l)
  def genObject(listSize: Int): Gen[JObject] = for (l <- genFieldList(listSize)) yield JObject(l)

  def genList(listSize: Int) = Gen.containerOfN[List, JValue](listSize, genJValue)
  def genFieldList(listSize: Int) = Gen.containerOfN[List, JField](listSize, genField)
  def genField = for (name <- alphaStr; value <- genJValue; id <- choose(0, 1000000)) yield JField(name+id, value)

  def genJValueClass: Gen[Class[_ <: JValue]] = oneOf(
    JNull.getClass.asInstanceOf[Class[JValue]], JNothing.getClass.asInstanceOf[Class[JValue]], classOf[JInt],
    classOf[JDouble], classOf[JBool], classOf[JString], classOf[JField], classOf[JArray], classOf[JObject])

  def listSize = choose(0, 5).sample.get

  implicit def arbJValue: Arbitrary[JValue] = Arbitrary(genJValue)
  implicit def arbJObject: Arbitrary[JObject] = Arbitrary(choose(0, 5) flatMap genObject)
  implicit def arbJValueClass: Arbitrary[Class[_ <: JValue]] = Arbitrary(genJValueClass)
  implicit def shrinkJValueClass[T]: Shrink[T] = Shrink(x => Stream.empty)
}
