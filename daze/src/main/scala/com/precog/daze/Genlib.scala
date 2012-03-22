package com.precog

package daze

import bytecode.Library

import bytecode.BuiltInFunc1

import java.lang.Math

import java.lang.String

import bytecode.BuiltInFunc2

import yggdrasil._

trait Genlib extends ImplLibrary with GenOpcode {
  override def _lib1 = super._lib1 ++ Set(sinh, toDegrees, expm1, getExponent, asin, log10, cos, exp, cbrt, atan, ceil, rint, log1p, sqrt, floor, toRadians, tanh, round, cosh, tan, abs, sin, nextUp, log, signum, acos, ulp)

  override def _lib2 = super._lib2 ++ Set(nextAfter, min, hypot, pow, max, atan2, copySign, IEEEremainder)

  object sinh extends BIF1(Vector("std", "math"), "sinh") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.sinh(num.toDouble))
    }
  }
  object toDegrees extends BIF1(Vector("std", "math"), "toDegrees") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.toDegrees(num.toDouble))
    }
  }
  object expm1 extends BIF1(Vector("std", "math"), "expm1") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.expm1(num.toDouble))
    }
  }
  object getExponent extends BIF1(Vector("std", "math"), "getExponent") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.getExponent(num.toDouble))
    }
  }
  object asin extends BIF1(Vector("std", "math"), "asin") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.asin(num.toDouble))
    }
  }
  object log10 extends BIF1(Vector("std", "math"), "log10") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.log10(num.toDouble))
    }
  }
  object cos extends BIF1(Vector("std", "math"), "cos") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.cos(num.toDouble))
    }
  }
  object exp extends BIF1(Vector("std", "math"), "exp") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.exp(num.toDouble))
    }
  }
  object cbrt extends BIF1(Vector("std", "math"), "cbrt") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.cbrt(num.toDouble))
    }
  }
  object atan extends BIF1(Vector("std", "math"), "atan") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.atan(num.toDouble))
    }
  }
  object ceil extends BIF1(Vector("std", "math"), "ceil") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.ceil(num.toDouble))
    }
  }
  object rint extends BIF1(Vector("std", "math"), "rint") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.rint(num.toDouble))
    }
  }
  object log1p extends BIF1(Vector("std", "math"), "log1p") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.log1p(num.toDouble))
    }
  }
  object sqrt extends BIF1(Vector("std", "math"), "sqrt") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.sqrt(num.toDouble))
    }
  }
  object floor extends BIF1(Vector("std", "math"), "floor") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.floor(num.toDouble))
    }
  }
  object toRadians extends BIF1(Vector("std", "math"), "toRadians") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.toRadians(num.toDouble))
    }
  }
  object tanh extends BIF1(Vector("std", "math"), "tanh") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.tanh(num.toDouble))
    }
  }
  object round extends BIF1(Vector("std", "math"), "round") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.round(num.toDouble))
    }
  }
  object cosh extends BIF1(Vector("std", "math"), "cosh") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.cosh(num.toDouble))
    }
  }
  object tan extends BIF1(Vector("std", "math"), "tan") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.tan(num.toDouble))
    }
  }
  object abs extends BIF1(Vector("std", "math"), "abs") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.abs(num.toDouble))
    }
  }
  object sin extends BIF1(Vector("std", "math"), "sin") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.sin(num.toDouble))
    }
  }
  object nextUp extends BIF1(Vector("std", "math"), "nextUp") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.nextUp(num.toDouble))
    }
  }
  object log extends BIF1(Vector("std", "math"), "log") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.log(num.toDouble))
    }
  }
  object signum extends BIF1(Vector("std", "math"), "signum") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.signum(num.toDouble))
    }
  }
  object acos extends BIF1(Vector("std", "math"), "acos") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.acos(num.toDouble))
    }
  }
  object ulp extends BIF1(Vector("std", "math"), "ulp") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.ulp(num.toDouble))
    }
  }
  object nextAfter extends BIF2(Vector("std", "math"), "nextAfter") {
    val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.nextAfter(num1.toDouble, num2.toDouble))
    }
  }
  object min extends BIF2(Vector("std", "math"), "min") {
    val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.min(num1.toDouble, num2.toDouble))
    }
  }
  object hypot extends BIF2(Vector("std", "math"), "hypot") {
    val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.hypot(num1.toDouble, num2.toDouble))
    }
  }
  object pow extends BIF2(Vector("std", "math"), "pow") {
    val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.pow(num1.toDouble, num2.toDouble))
    }
  }
  object max extends BIF2(Vector("std", "math"), "max") {
    val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.max(num1.toDouble, num2.toDouble))
    }
  }
  object atan2 extends BIF2(Vector("std", "math"), "atan2") {
    val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.atan2(num1.toDouble, num2.toDouble))
    }
  }
  object copySign extends BIF2(Vector("std", "math"), "copySign") {
    val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.copySign(num1.toDouble, num2.toDouble))
    }
  }
  object IEEEremainder extends BIF2(Vector("std", "math"), "IEEEremainder") {
    val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.IEEEremainder(num1.toDouble, num2.toDouble))
    }
  }
}
