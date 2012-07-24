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

import bytecode.{ BinaryOperationType, UnaryOperationType, JNumberT }
import bytecode.Library

import yggdrasil._
import yggdrasil.table._

trait MathLib[M[+_]] extends GenOpcode[M] {
  val MathNamespace = Vector("std", "math")

  override def _lib1 = super._lib1 ++ Set(sinh, toDegrees, expm1, getExponent, asin, log10, cos, exp, cbrt, atan, ceil, rint, log1p, sqrt, floor, toRadians, tanh, round, cosh, tan, abs, sin, log, signum, acos, ulp)

  override def _lib2 = super._lib2 ++ Set(min, hypot, pow, max, atan2, copySign, IEEEremainder)

  object sinh extends Op1(MathNamespace, "sinh") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.sinh(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.sinh(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.sinh(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(math.sinh(num.toDouble))
    } */
  }
  object toDegrees extends Op1(MathNamespace, "toDegrees") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.toDegrees(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.toDegrees(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.toDegrees(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(math.toDegrees(num.toDouble))
    } */
  }
  object expm1 extends Op1(MathNamespace, "expm1") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.expm1(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.expm1(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.expm1(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(math.expm1(num.toDouble))
    } */
  }
  object getExponent extends Op1(MathNamespace, "getExponent") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) > 0
        def apply(row: Int) = math.floor(math.log(c(row)) / math.log(2))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) > 0
        def apply(row: Int) = math.floor(math.log(c(row).toDouble) / math.log(2))
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) > 0
        def apply(row: Int) = math.floor(math.log(c(row).toDouble) / math.log(2))
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(math.getExponent(num.toDouble))
    } */
  }
  object asin extends Op1(MathNamespace, "asin") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && (-1 <= c(row)) && (c(row) <= 1)
        def apply(row: Int) = math.asin(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && (-1 <= c(row)) && (c(row) <= 1)
        def apply(row: Int) = math.asin(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && (-1 <= c(row)) && (c(row) <= 1)
        def apply(row: Int) = math.asin(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) if ((-1 <= num) && (num <= 1)) => 
        SDecimal(math.asin(num.toDouble))
    } */
  }
  object log10 extends Op1(MathNamespace, "log10") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) > 0
        def apply(row: Int) = math.log10(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) > 0
        def apply(row: Int) = math.log10(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) > 0
        def apply(row: Int) = math.log10(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) if (num > 0) => 
        SDecimal(math.log10(num.toDouble))
    } */
  }
  object cos extends Op1(MathNamespace, "cos") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.cos(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.cos(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.cos(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(math.cos(num.toDouble))
    } */
  }
  object exp extends Op1(MathNamespace, "exp") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.exp(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.exp(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.exp(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(math.exp(num.toDouble))
    } */
  }
  object cbrt extends Op1(MathNamespace, "cbrt") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.cbrt(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.cbrt(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.cbrt(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(math.cbrt(num.toDouble))
    } */
  }
  object atan extends Op1(MathNamespace, "atan") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.atan(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.atan(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.atan(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(math.atan(num.toDouble))
    } */
  }
  object ceil extends Op1(MathNamespace, "ceil") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.ceil(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.ceil(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.ceil(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(math.ceil(num.toDouble))
    } */
  }
  object rint extends Op1(MathNamespace, "rint") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.rint(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.rint(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.rint(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(math.rint(num.toDouble))
    } */
  }
  object log1p extends Op1(MathNamespace, "log1p") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) > -1
        def apply(row: Int) = math.log1p(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) > -1
        def apply(row: Int) = math.log1p(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) > -1
        def apply(row: Int) = math.log1p(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) if (num > -1) => 
        SDecimal(math.log1p(num.toDouble))
    } */
  }
  object sqrt extends Op1(MathNamespace, "sqrt") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) >= 0
        def apply(row: Int) = math.sqrt(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) >= 0
        def apply(row: Int) = math.sqrt(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) >= 0
        def apply(row: Int) = math.sqrt(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) if (num >= 0) => 
        SDecimal(math.sqrt(num.toDouble))
    } */
  }
  object floor extends Op1(MathNamespace, "floor") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.floor(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.floor(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.floor(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(math.floor(num.toDouble))
    } */
  }
  object toRadians extends Op1(MathNamespace, "toRadians") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.toRadians(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.toRadians(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.toRadians(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(math.toRadians(num.toDouble))
    } */
  }
  object tanh extends Op1(MathNamespace, "tanh") {val operandType = Some(SDecimal)
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.tanh(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.tanh(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.tanh(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(math.tanh(num.toDouble))
    } */
  }
  object round extends Op1(MathNamespace, "round") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.round(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.round(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.round(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(math.round(num.toDouble))
    } */
  }
  object cosh extends Op1(MathNamespace, "cosh") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.cosh(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.cosh(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.cosh(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(math.cosh(num.toDouble))
    } */
  }
  object tan extends Op1(MathNamespace, "tan") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && (!(c(row) % (math.Pi / 2) == 0) || (c(row) % (math.Pi) == 0))
        def apply(row: Int) = math.tan(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && (!(c(row) % (math.Pi / 2) == 0) || (c(row) % (math.Pi) == 0))
        def apply(row: Int) = math.tan(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && (!(c(row) % (math.Pi / 2) == 0) || (c(row) % (math.Pi) == 0))
        def apply(row: Int) = math.tan(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) if (!(num % (math.Pi / 2) == 0) || (num % (math.Pi) == 0)) => 
        SDecimal(math.tan(num.toDouble))
    } */
  }
  object abs extends Op1(MathNamespace, "abs") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.abs(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.abs(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.abs(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(math.abs(num.toDouble))
    } */
  }
  object sin extends Op1(MathNamespace, "sin") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.sin(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.sin(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.sin(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(math.sin(num.toDouble))
    } */
  }
  object log extends Op1(MathNamespace, "log") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) > 0
        def apply(row: Int) = math.log(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) > 0
        def apply(row: Int) = math.log(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) > 0
        def apply(row: Int) = math.log(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) if (num > 0) => 
        SDecimal(math.log(num.toDouble))
    } */
  }
  object signum extends Op1(MathNamespace, "signum") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.signum(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.signum(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.signum(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(math.signum(num.toDouble))
    } */
  }
  object acos extends Op1(MathNamespace, "acos") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && (-1 <= c(row)) && (c(row) <= 1)
        def apply(row: Int) = math.acos(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && (-1 <= c(row)) && (c(row) <= 1)
        def apply(row: Int) = math.acos(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && (-1 <= c(row)) && (c(row) <= 1)
        def apply(row: Int) = math.acos(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) if ((-1 <= num) && (num <= 1))=> 
        SDecimal(math.acos(num.toDouble))
    } */
  }
  object ulp extends Op1(MathNamespace, "ulp") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.ulp(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.ulp(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = math.ulp(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(math.ulp(num.toDouble))
    } */
  }
  object min extends Op2(MathNamespace, "min") {
    val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)
    def f2: F2 = new CF2P({
      case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.min(c1(row), c2(row))
      }
      case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.min(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.min(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.min(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.min(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.min(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.min(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.min(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.min(c1(row).toDouble, c2(row).toDouble)
      }
    })
    
    /* val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(math.min(num1.toDouble, num2.toDouble))
    } */
  }
  object hypot extends Op2(MathNamespace, "hypot") {
    val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)
    def f2: F2 = new CF2P({
      case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.hypot(c1(row), c2(row))
      }
      case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.hypot(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.hypot(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.hypot(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.hypot(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.hypot(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.hypot(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.hypot(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.hypot(c1(row).toDouble, c2(row).toDouble)
      }
    })
    
    /* val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(math.hypot(num1.toDouble, num2.toDouble))
    } */
  }
  object pow extends Op2(MathNamespace, "pow") {
    val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)
    def f2: F2 = new CF2P({
      case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.pow(c1(row), c2(row))
      }
      case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.pow(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.pow(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.pow(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.pow(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.pow(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.pow(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.pow(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.pow(c1(row).toDouble, c2(row).toDouble)
      }
    })
    
    /* val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(math.pow(num1.toDouble, num2.toDouble))
    } */
  }
  object max extends Op2(MathNamespace, "max") {
    val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)
    def f2: F2 = new CF2P({
      case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.max(c1(row), c2(row))
      }
      case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.max(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.max(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.max(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.max(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.max(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.max(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.max(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.max(c1(row).toDouble, c2(row).toDouble)
      }
    })
    
    /* val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(math.max(num1.toDouble, num2.toDouble))
    } */
  }
  object atan2 extends Op2(MathNamespace, "atan2") {
    val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)
    def f2: F2 = new CF2P({
      case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.atan2(c1(row), c2(row))
      }
      case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.atan2(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.atan2(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.atan2(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.atan2(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.atan2(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.atan2(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.atan2(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.atan2(c1(row).toDouble, c2(row).toDouble)
      }
    })
    
    /* val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(math.atan2(num1.toDouble, num2.toDouble))
    } */
  }
  object copySign extends Op2(MathNamespace, "copySign") {  //TODO deal with 2's complement; ensure proper column return types for each case
    val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)
    def f2: F2 = new CF2P({
      case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = {
          if (math.signum(c1(row)) == math.signum(c2(row))) c1(row)
          else -1 * c1(row)
        }
      }
      case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = {
          if (math.signum(c1(row).toDouble) == math.signum(c2(row).toDouble)) c1(row)
          else -1 * c1(row).toDouble
        }
      }
      case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = {
          if (math.signum(c1(row).toDouble) == math.signum(c2(row).toDouble)) c1(row)
          else -1 * c1(row).toDouble
        }
      }
      case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = {
          if (math.signum(c1(row).toDouble) == math.signum(c2(row).toDouble)) c1(row)
          else -1 * c1(row).toDouble
        }
      }
      case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = {
          if (math.signum(c1(row).toDouble) == math.signum(c2(row).toDouble)) c1(row).toDouble
          else -1 * c1(row).toDouble
        }
      }
      case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = {
          if (math.signum(c1(row).toDouble) == math.signum(c2(row).toDouble)) c1(row)
          else -1 * c1(row).toDouble
        }
      }
      case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = {
          if (math.signum(c1(row).toDouble) == math.signum(c2(row).toDouble)) c1(row)
          else -1 * c1(row).toDouble
        }
      }
      case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = {
          if (math.signum(c1(row).toDouble) == math.signum(c2(row).toDouble)) c1(row).toDouble
          else -1 * c1(row).toDouble
        }
      }
      case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = {
          if (math.signum(c1(row).toDouble) == math.signum(c2(row).toDouble)) c1(row).toDouble
          else -1 * c1(row).toDouble
        }
      }
    })
    
    /* val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(math.copySign(num1.toDouble, num2.toDouble))
    } */
  }
  object IEEEremainder extends Op2(MathNamespace, "IEEEremainder") {
    val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)
    def f2: F2 = new CF2P({
      case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.IEEEremainder(c1(row), c2(row))
      }
      case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.IEEEremainder(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.IEEEremainder(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.IEEEremainder(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.IEEEremainder(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.IEEEremainder(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.IEEEremainder(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.IEEEremainder(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = math.IEEEremainder(c1(row).toDouble, c2(row).toDouble)
      }
    })
    
    /* val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(math.IEEEremainder(num1.toDouble, num2.toDouble))
    } */
  }
}
