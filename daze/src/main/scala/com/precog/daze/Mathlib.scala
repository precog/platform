package com.precog
package daze

import bytecode.Library
import bytecode.BuiltInFunc1
import bytecode.BuiltInFunc2

import java.lang.Math

import yggdrasil._

object Mathlib extends Mathlib

trait Mathlib extends GenOpcode with ImplLibrary {
  val MathNamespace = Vector("std", "math")

  override def _lib1 = super._lib1 ++ Set(sinh, toDegrees, expm1, getExponent, asin, log10, cos, exp, cbrt, atan, ceil, rint, log1p, sqrt, floor, toRadians, tanh, round, cosh, tan, abs, sin, nextUp, log, signum, acos, ulp)

  override def _lib2 = super._lib2 ++ Set(nextAfter, min, hypot, pow, max, atan2, copySign, IEEEremainder)

  object sinh extends BIF1(MathNamespace, "sinh") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.sinh(num.toDouble))
    }
  }
  object toDegrees extends BIF1(MathNamespace, "toDegrees") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.toDegrees(num.toDouble))
    }
  }
  object expm1 extends BIF1(MathNamespace, "expm1") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.expm1(num.toDouble))
    }
  }
  object getExponent extends BIF1(MathNamespace, "getExponent") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.getExponent(num.toDouble))
    }
  }
  object asin extends BIF1(MathNamespace, "asin") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) if ((-1 <= num) && (num <= 1)) => 
        SDecimal(Math.asin(num.toDouble))
    }
  }
  object log10 extends BIF1(MathNamespace, "log10") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) if (num > 0) => 
        SDecimal(Math.log10(num.toDouble))
    }
  }
  object cos extends BIF1(MathNamespace, "cos") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.cos(num.toDouble))
    }
  }
  object exp extends BIF1(MathNamespace, "exp") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.exp(num.toDouble))
    }
  }
  object cbrt extends BIF1(MathNamespace, "cbrt") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.cbrt(num.toDouble))
    }
  }
  object atan extends BIF1(MathNamespace, "atan") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.atan(num.toDouble))
    }
  }
  object ceil extends BIF1(MathNamespace, "ceil") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.ceil(num.toDouble))
    }
  }
  object rint extends BIF1(MathNamespace, "rint") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.rint(num.toDouble))
    }
  }
  object log1p extends BIF1(MathNamespace, "log1p") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) if (num > -1) => 
        SDecimal(Math.log1p(num.toDouble))
    }
  }
  object sqrt extends BIF1(MathNamespace, "sqrt") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) if (num >= 0) => 
        SDecimal(Math.sqrt(num.toDouble))
    }
  }
  object floor extends BIF1(MathNamespace, "floor") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.floor(num.toDouble))
    }
  }
  object toRadians extends BIF1(MathNamespace, "toRadians") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.toRadians(num.toDouble))
    }
  }
  object tanh extends BIF1(MathNamespace, "tanh") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.tanh(num.toDouble))
    }
  }
  object round extends BIF1(MathNamespace, "round") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.round(num.toDouble))
    }
  }
  object cosh extends BIF1(MathNamespace, "cosh") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.cosh(num.toDouble))
    }
  }
  object tan extends BIF1(MathNamespace, "tan") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) if (!(num % (Math.PI / 2) == 0) || (num % (Math.PI) == 0)) => 
        SDecimal(Math.tan(num.toDouble))
    }
  }
  object abs extends BIF1(MathNamespace, "abs") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.abs(num.toDouble))
    }
  }
  object sin extends BIF1(MathNamespace, "sin") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.sin(num.toDouble))
    }
  }
  object nextUp extends BIF1(MathNamespace, "nextUp") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.nextUp(num.toDouble))
    }
  }
  object log extends BIF1(MathNamespace, "log") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) if (num > 0) => 
        SDecimal(Math.log(num.toDouble))
    }
  }
  object signum extends BIF1(MathNamespace, "signum") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.signum(num.toDouble))
    }
  }
  object acos extends BIF1(MathNamespace, "acos") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) if ((-1 <= num) && (num <= 1))=> 
        SDecimal(Math.acos(num.toDouble))
    }
  }
  object ulp extends BIF1(MathNamespace, "ulp") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(Math.ulp(num.toDouble))
    }
  }
  object nextAfter extends BIF2(MathNamespace, "nextAfter") {
    val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.nextAfter(num1.toDouble, num2.toDouble))
    }
  }
  object min extends BIF2(MathNamespace, "min") {
    val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.min(num1.toDouble, num2.toDouble))
    }
  }
  object hypot extends BIF2(MathNamespace, "hypot") {
    val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.hypot(num1.toDouble, num2.toDouble))
    }
  }
  object pow extends BIF2(MathNamespace, "pow") {
    val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.pow(num1.toDouble, num2.toDouble))
    }
  }
  object max extends BIF2(MathNamespace, "max") {
    val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.max(num1.toDouble, num2.toDouble))
    }
  }
  object atan2 extends BIF2(MathNamespace, "atan2") {
    val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.atan2(num1.toDouble, num2.toDouble))
    }
  }
  object copySign extends BIF2(MathNamespace, "copySign") {
    val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.copySign(num1.toDouble, num2.toDouble))
    }
  }
  object IEEEremainder extends BIF2(MathNamespace, "IEEEremainder") {
    val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = {
      case (SDecimal(num1), SDecimal(num2)) => SDecimal(Math.IEEEremainder(num1.toDouble, num2.toDouble))
    }
  }
}
