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