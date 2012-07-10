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

import java.lang.Math

import yggdrasil._
import yggdrasil.table._

object MathLib extends MathLib

trait MathLib extends GenOpcode with ImplLibrary {
  val MathNamespace = Vector("std", "math")

  override def _lib1 = super._lib1 ++ Set(sinh, toDegrees, expm1, getExponent, asin, log10, cos, exp, cbrt, atan, ceil, rint, log1p, sqrt, floor, toRadians, tanh, round, cosh, tan, abs, sin, nextUp, log, signum, acos, ulp)

  override def _lib2 = super._lib2 ++ Set(nextAfter, min, hypot, pow, max, atan2, copySign, IEEEremainder)

  object sinh extends Op1(MathNamespace, "sinh") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.sinh(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.sinh(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.sinh(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.sinh(num.toDouble))
    } */
  }
  object toDegrees extends Op1(MathNamespace, "toDegrees") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.toDegrees(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.toDegrees(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.toDegrees(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.toDegrees(num.toDouble))
    } */
  }
  object expm1 extends Op1(MathNamespace, "expm1") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.expm1(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.expm1(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.expm1(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.expm1(num.toDouble))
    } */
  }
  object getExponent extends Op1(MathNamespace, "getExponent") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.getExponent(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.getExponent(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.getExponent(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.getExponent(num.toDouble))
    } */
  }
  object asin extends Op1(MathNamespace, "asin") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && (-1 <= c(row)) && (c(row) <= 1)
        def apply(row: Int) = Math.asin(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && (-1 <= c(row)) && (c(row) <= 1)
        def apply(row: Int) = Math.asin(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && (-1 <= c(row)) && (c(row) <= 1)
        def apply(row: Int) = Math.asin(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) if ((-1 <= num) && (num <= 1)) => 
        SDecimal(Math.asin(num.toDouble))
    } */
  }
  object log10 extends Op1(MathNamespace, "log10") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) > 0
        def apply(row: Int) = Math.log10(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) > 0
        def apply(row: Int) = Math.log10(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) > 0
        def apply(row: Int) = Math.log10(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) if (num > 0) => 
        SDecimal(Math.log10(num.toDouble))
    } */
  }
  object cos extends Op1(MathNamespace, "cos") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.cos(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.cos(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.cos(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.cos(num.toDouble))
    } */
  }
  object exp extends Op1(MathNamespace, "exp") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.exp(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.exp(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.exp(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.exp(num.toDouble))
    } */
  }
  object cbrt extends Op1(MathNamespace, "cbrt") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.cbrt(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.cbrt(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.cbrt(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.cbrt(num.toDouble))
    } */
  }
  object atan extends Op1(MathNamespace, "atan") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.atan(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.atan(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.atan(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.atan(num.toDouble))
    } */
  }
  object ceil extends Op1(MathNamespace, "ceil") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.ceil(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.ceil(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.ceil(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.ceil(num.toDouble))
    } */
  }
  object rint extends Op1(MathNamespace, "rint") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.rint(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.rint(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.rint(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.rint(num.toDouble))
    } */
  }
  object log1p extends Op1(MathNamespace, "log1p") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) > -1
        def apply(row: Int) = Math.log1p(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) > -1
        def apply(row: Int) = Math.log1p(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) > -1
        def apply(row: Int) = Math.log1p(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) if (num > -1) => 
        SDecimal(Math.log1p(num.toDouble))
    } */
  }
  object sqrt extends Op1(MathNamespace, "sqrt") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) >= 0
        def apply(row: Int) = Math.sqrt(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) >= 0
        def apply(row: Int) = Math.sqrt(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) >= 0
        def apply(row: Int) = Math.sqrt(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) if (num >= 0) => 
        SDecimal(Math.sqrt(num.toDouble))
    } */
  }
  object floor extends Op1(MathNamespace, "floor") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.floor(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.floor(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.floor(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.floor(num.toDouble))
    } */
  }
  object toRadians extends Op1(MathNamespace, "toRadians") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.toRadians(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.toRadians(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.toRadians(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.toRadians(num.toDouble))
    } */
  }
  object tanh extends Op1(MathNamespace, "tanh") {val operandType = Some(SDecimal)
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.tanh(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.tanh(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.tanh(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.tanh(num.toDouble))
    } */
  }
  object round extends Op1(MathNamespace, "round") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.round(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.round(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.round(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.round(num.toDouble))
    } */
  }
  object cosh extends Op1(MathNamespace, "cosh") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.cosh(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.cosh(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.cosh(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.cosh(num.toDouble))
    } */
  }
  object tan extends Op1(MathNamespace, "tan") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && (!(c(row) % (Math.PI / 2) == 0) || (c(row) % (Math.PI) == 0))
        def apply(row: Int) = Math.tan(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && (!(c(row) % (Math.PI / 2) == 0) || (c(row) % (Math.PI) == 0))
        def apply(row: Int) = Math.tan(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && (!(c(row) % (Math.PI / 2) == 0) || (c(row) % (Math.PI) == 0))
        def apply(row: Int) = Math.tan(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) if (!(num % (Math.PI / 2) == 0) || (num % (Math.PI) == 0)) => 
        SDecimal(Math.tan(num.toDouble))
    } */
  }
  object abs extends Op1(MathNamespace, "abs") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.abs(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.abs(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.abs(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.abs(num.toDouble))
    } */
  }
  object sin extends Op1(MathNamespace, "sin") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.sin(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.sin(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.sin(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.sin(num.toDouble))
    } */
  }
  object nextUp extends Op1(MathNamespace, "nextUp") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.nextUp(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.nextUp(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.nextUp(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.nextUp(num.toDouble))
    } */
  }
  object log extends Op1(MathNamespace, "log") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) > 0
        def apply(row: Int) = Math.log(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) > 0
        def apply(row: Int) = Math.log(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row) > 0
        def apply(row: Int) = Math.log(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) if (num > 0) => 
        SDecimal(Math.log(num.toDouble))
    } */
  }
  object signum extends Op1(MathNamespace, "signum") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.signum(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.signum(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.signum(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.signum(num.toDouble))
    } */
  }
  object acos extends Op1(MathNamespace, "acos") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && (-1 <= c(row)) && (c(row) <= 1)
        def apply(row: Int) = Math.acos(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && (-1 <= c(row)) && (c(row) <= 1)
        def apply(row: Int) = Math.acos(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        override def isDefinedAt(row: Int) = c.isDefinedAt(row) && (-1 <= c(row)) && (c(row) <= 1)
        def apply(row: Int) = Math.acos(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) if ((-1 <= num) && (num <= 1))=> 
        SDecimal(Math.acos(num.toDouble))
    } */
  }
  object ulp extends Op1(MathNamespace, "ulp") {
    def f1: F1 = new CF1P({
      case c: DoubleColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.ulp(c(row))
      }
      case c: LongColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.ulp(c(row).toDouble)
      }
      case c: NumColumn => new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = Math.ulp(c(row).toDouble)
      }
    })
    
    /* val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.ulp(num.toDouble))
    } */
  }
  object nextAfter extends Op2(MathNamespace, "nextAfter") {   //AHHHHHHHH nine cases AHHHHHHHHHHH
    def f2: F2 = new CF2P({
      case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
    })
    
    /* val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.nextAfter(num1.toDouble, num2.toDouble))
    } */
  }
  object min extends Op2(MathNamespace, "min") {
    def f2: F2 = new CF2P({
      case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.min(c1(row), c2(row))
      }
      case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
    })
    
    /* val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.min(num1.toDouble, num2.toDouble))
    } */
  }
  object hypot extends Op2(MathNamespace, "hypot") {
    def f2: F2 = new CF2P({
      case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.hypot(c1(row), c2(row))
      }
      case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
    })
    
    /* val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.hypot(num1.toDouble, num2.toDouble))
    } */
  }
  object pow extends Op2(MathNamespace, "pow") {
    def f2: F2 = new CF2P({
      case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.pow(c1(row), c2(row))
      }
      case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
    })
    
    /* val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.pow(num1.toDouble, num2.toDouble))
    } */
  }
  object max extends Op2(MathNamespace, "max") {
    def f2: F2 = new CF2P({
      case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.max(c1(row), c2(row))
      }
      case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
    })
    
    /* val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.max(num1.toDouble, num2.toDouble))
    } */
  }
  object atan2 extends Op2(MathNamespace, "atan2") {
    def f2: F2 = new CF2P({
      case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.atan2(c1(row), c2(row))
      }
      case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
    })
    
    /* val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.atan2(num1.toDouble, num2.toDouble))
    } */
  }
  object copySign extends Op2(MathNamespace, "copySign") {
    def f2: F2 = new CF2P({
      case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.copySign(c1(row), c2(row))
      }
      case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
    })
    
    /* val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.copySign(num1.toDouble, num2.toDouble))
    } */
  }
  object IEEEremainder extends Op2(MathNamespace, "IEEEremainder") {
    def f2: F2 = new CF2P({
      case (c1: DoubleColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.IEEEremainder(c1(row), c2(row))
      }
      case (c1: DoubleColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: DoubleColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: DoubleColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: LongColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: LongColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
      case (c1: NumColumn, c2: NumColumn) => new Map2Column(c1, c2) with DoubleColumn {
        def apply(row: Int) = Math.nextAfter(c1(row).toDouble, c2(row).toDouble)
      }
    })
    
    /* val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.IEEEremainder(num1.toDouble, num2.toDouble))
    } */
  }
}
