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
package com.precog
package daze

import bytecode.Library
import bytecode.BuiltInFunc1
import bytecode.BuiltInFunc2
import yggdrasil._

object InfixLib extends InfixLib

trait InfixLib extends ImplLibrary with GenOpcode {
  object Infix {
    val InfixNamespace = Vector("std", "infix")

    object Add extends BIF2(InfixNamespace, "add") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SDecimal(l), SDecimal(r)) => SDecimal(l + r)
      }

      val operandType = (Some(SDecimal), Some(SDecimal))
    }

    object Sub extends BIF2(InfixNamespace, "subtract") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SDecimal(l), SDecimal(r)) => SDecimal(l - r)
      }

      val operandType = (Some(SDecimal), Some(SDecimal))
    }

    object Mul extends BIF2(InfixNamespace, "multiply") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SDecimal(l), SDecimal(r)) => SDecimal(l * r)
      }

      val operandType = (Some(SDecimal), Some(SDecimal))
    }

    object Div extends BIF2(InfixNamespace, "divide") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SDecimal(l), SDecimal(r)) if r != BigDecimal(0) => SDecimal(l / r)
      }

      val operandType = (Some(SDecimal), Some(SDecimal))
    }

    object Lt extends BIF2(InfixNamespace, "lt") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SDecimal(l), SDecimal(r)) => SBoolean(l < r)
      }

      val operandType = (Some(SDecimal), Some(SDecimal))
    }

    object LtEq extends BIF2(InfixNamespace, "lte") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SDecimal(l), SDecimal(r)) => SBoolean(l <= r)
      }

      val operandType = (Some(SDecimal), Some(SDecimal))
    }

    object Gt extends BIF2(InfixNamespace, "gt") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SDecimal(l), SDecimal(r)) => SBoolean(l > r)
      }

      val operandType = (Some(SDecimal), Some(SDecimal))
    }

    object GtEq extends BIF2(InfixNamespace, "gte") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SDecimal(l), SDecimal(r)) => SBoolean(l >= r)
      }

      val operandType = (Some(SDecimal), Some(SDecimal))
    }

    object Eq extends BIF2(InfixNamespace, "eq") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (a, b) => SBoolean(a == b)
      }

      val operandType = (None, None)
    }

    object NotEq extends BIF2(InfixNamespace, "ne") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (a, b) => SBoolean(a != b)
      }

      val operandType = (None, None)
    }

    object And extends BIF2(InfixNamespace, "and") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (v1: SBooleanValue, v2: SBooleanValue) => v1 && v2
      }

      val operandType = (Some(SBoolean), Some(SBoolean))
    }

    object Or extends BIF2(InfixNamespace, "or") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (v1: SBooleanValue, v2: SBooleanValue) => v1 || v2
      }

      val operandType = (Some(SBoolean), Some(SBoolean))
    }

    object WrapObject extends BIF2(InfixNamespace, "wrap") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SString(key), value) => SObject(Map(key -> value))
      }

      val operandType = (Some(SString), None)
    }

    object JoinObject extends BIF2(InfixNamespace, "merge") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SObject(left), SObject(right)) => SObject(left ++ right)
      }

      val operandType = (Some(SObject), Some(SObject))
    }

    object JoinArray extends BIF2(InfixNamespace, "concat") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SArray(left), SArray(right)) => SArray(left ++ right)
      }

      val operandType = (Some(SArray), Some(SArray))
    }

    object ArraySwap extends BIF2(InfixNamespace, "swap") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SArray(arr), SDecimal(i)) if i.isValidInt && (i.toInt >= 0 && i.toInt < arr.length) => 
          val (left, right) = arr splitAt i.toInt
          SArray(left.init ++ Vector(right.head, left.last) ++ right.tail)
      }

      val operandType = (Some(SArray), Some(SDecimal))
    }

    object DerefObject extends BIF2(InfixNamespace, "get") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SObject(obj), SString(key)) if obj.contains(key) => obj(key)
      }

      val operandType = (Some(SObject), Some(SString))
    }

    object DerefArray extends BIF2(InfixNamespace, "valueAt") {
      val operation: PartialFunction[(SValue, SValue), SValue] = {
        case (SArray(arr), SDecimal(i)) if i.isValidInt && arr.length < i.toInt => arr(i.toInt)
      }

      val operandType = (Some(SArray), Some(SDecimal))
    }
  }
}

