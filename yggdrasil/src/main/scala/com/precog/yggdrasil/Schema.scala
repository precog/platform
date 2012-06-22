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

trait Schema {
  sealed trait SType
  
  case object SNumber extends SType
  case object SText extends SType
  case object SBoolean extends SType
  case object SNull extends SType
  case object SUnknown extends SType          // ahhhhhhhhh!!!!
  
  case class SArray(tpe: SType) extends SType
  case class SObject(fields: Map[String, SType]) extends SType
  case class SUnion(left: SType, right: SType) extends SType
  
  def flattenUnions(tpe: SType): Set[SType] = tpe match {
    case SUnion(left, right) => flattenUnions(left) ++ flattenUnions(right)
    case t => Set(t)
  }
}
