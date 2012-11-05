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
package com.precog.common.util

import blueeyes.json._
import org.scalacheck._
import Gen._
import Arbitrary.arbitrary

trait ArbitraryJValue {
  def genJValue:  Gen[JValue]  = frequency((10, genSimple), (1, wrap(choose(0, 5) flatMap genArray)), (1, wrap(choose(0, 5) flatMap genObject)))
  def genJNum:    Gen[JNum]    = arbitrary[BigDecimal].map(JNum(_))
  def genJBool:   Gen[JBool]   = arbitrary[Boolean].map(JBool(_))
  def genJString: Gen[JString] = alphaStr.map(JString(_))
  def genSimple: Gen[JValue] = oneOf(
    value(JNull),
    genJNum,
    genJBool,
    genJString)
    
  def genSimpleNotNull: Gen[JValue] = oneOf(
    genJNum,
    genJBool,
    genJString)

  def genArray(listSize: Int): Gen[JValue] = for (l <- genList(listSize)) yield JArray(l)
  def genObject(listSize: Int): Gen[JObject] = for (l <- genFieldList(listSize)) yield JObject(l)

  def genList(listSize: Int) = Gen.containerOfN[List, JValue](listSize, genJValue)
  def genFieldList(listSize: Int) = Gen.containerOfN[List, JField](listSize, genField)
  def genField = for (name <- alphaStr; value <- genJValue; id <- choose(0, 1000000)) yield JField(name+id, value)

  def genJValueClass: Gen[Class[_ <: JValue]] = oneOf(
    JNull.getClass.asInstanceOf[Class[JValue]], 
    JUndefined.getClass.asInstanceOf[Class[JValue]], 
    classOf[JNum], 
    classOf[JBool], 
    classOf[JString], 
    classOf[JArray], 
    classOf[JObject]
  )

  def listSize = choose(0, 5).sample.get

  implicit def arbJValue: Arbitrary[JValue] = Arbitrary(genJValue)
  implicit def arbJObject: Arbitrary[JObject] = Arbitrary(choose(0, 5) flatMap genObject)
  implicit def arbJValueClass: Arbitrary[Class[_ <: JValue]] = Arbitrary(genJValueClass)
  implicit def shrinkJValueClass[T]: Shrink[T] = Shrink(x => Stream.empty)
  
  // BigDecimal *isn't* arbitrary precision!  AWESOME!!!
  implicit def arbBigDecimal: Arbitrary[BigDecimal] = Arbitrary(for {
    mantissa <- arbitrary[Long]
    exponent <- arbitrary[Int]
    
    adjusted = if (exponent.toLong + mantissa.toString.length >= Int.MaxValue.toLong)
      exponent - mantissa.toString.length
    else if (exponent.toLong - mantissa.toString.length <= Int.MinValue.toLong)
      exponent + mantissa.toString.length
    else
      exponent
  } yield BigDecimal(mantissa, adjusted, java.math.MathContext.UNLIMITED))
}
