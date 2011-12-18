package com.querio.ingest.util

import blueeyes.json.JsonAST
import org.scalacheck._
import Gen._
import Arbitrary.arbitrary

trait ArbitraryJValue {
  import JsonAST._

  def genJValue:  Gen[JValue]  = frequency((10, genSimple), (1, wrap(genArray)), (1, wrap(genObject)))
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

  def genArray: Gen[JValue] = for (l <- genList) yield JArray(l)
  def genObject: Gen[JObject] = for (l <- genFieldList) yield JObject(l)

  def genList = Gen.containerOfN[List, JValue](listSize, genJValue)
  def genFieldList = Gen.containerOfN[List, JField](listSize, genField)
  def genField = for (name <- alphaStr; value <- genJValue; id <- choose(0, 1000000)) yield JField(name+id, value)

  def genJValueClass: Gen[Class[_ <: JValue]] = oneOf(
    JNull.getClass.asInstanceOf[Class[JValue]], JNothing.getClass.asInstanceOf[Class[JValue]], classOf[JInt],
    classOf[JDouble], classOf[JBool], classOf[JString], classOf[JField], classOf[JArray], classOf[JObject])

  def listSize = choose(0, 5).sample.get

  implicit def arbJValue: Arbitrary[JValue] = Arbitrary(genJValue)
  implicit def arbJObject: Arbitrary[JObject] = Arbitrary(genObject)
  implicit def arbJValueClass: Arbitrary[Class[_ <: JValue]] = Arbitrary(genJValueClass)
  implicit def shrinkJValueClass[T]: Shrink[T] = Shrink(x => Stream.empty)
}