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
package com.reportgrid.common.json

sealed trait SJValue

sealed trait StorageType[A <: SJValue] {
  def as(v: SJValue)(implicit m: ClassManifest[A]): Option[A] = v match {
    case v if v.getClass.isAssignableFrom(m.erasure) => Some(v.asInstanceOf[A])
    case _ => None
  }
}

case class SJObject(fields: Map[String, SJValue]) extends SJValue
case object SJObject extends StorageType[SJObject] with (Map[String, SJValue] => SJObject)

case class SJArray(values: Vector[SJValue]) extends SJValue
case object SJArray extends StorageType[SJArray] with (Vector[SJValue] => SJArray)

case class SJString(value: String) extends SJValue
case object SJString extends StorageType[SJString] with (String => SJString)

case class SJBoolean(value: Boolean) extends SJValue
case object SJBoolean extends StorageType[SJBoolean] with (Boolean => SJBoolean)

case class SJLong(value: Long) extends SJValue
case object SJLong extends StorageType[SJLong] with (Long => SJLong)

case class SJDouble(value: Double) extends SJValue
case object SJDouble extends StorageType[SJDouble] with (Double => SJDouble)

case class SJDecimal(value: BigDecimal) extends SJValue
case object SJDecimal extends StorageType[SJDecimal] with (BigDecimal => SJDecimal)

case object SJNull extends SJValue with StorageType[SJValue]

case object SJEmptyObject extends StorageType[SJObject]
case object SJEmptyArray extends StorageType[SJArray]
