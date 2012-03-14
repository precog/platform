package com.precog

package daze

import yggdrasil._
import bytecode.BuiltInFunc1
import bytecode.BuiltInFunc2
import java.lang.Math
import bytecode.Library
trait Genlib extends Library {
  lazy val lib = _lib
  def _lib: Set[BIF1] = Set()
}
trait GenLibrary extends GenOpcode with Genlib {
  override def _lib = super._lib ++ Set(sinh, toDegrees, expm1, getExponent, asin, log10, cos, exp, cbrt, atan, ceil, rint, log1p, sqrt, floor, toRadians, tanh, round, cosh, tan, abs, sin, nextUp, log, signum, acos, ulp)

object sinh extends BIF1(Vector(), "sinh") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.sinh(num.toDouble))
  }
}
object toDegrees extends BIF1(Vector(), "toDegrees") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.toDegrees(num.toDouble))
  }
}
object expm1 extends BIF1(Vector(), "expm1") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.expm1(num.toDouble))
  }
}
object getExponent extends BIF1(Vector(), "getExponent") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.getExponent(num.toDouble))
  }
}
object asin extends BIF1(Vector(), "asin") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.asin(num.toDouble))
  }
}
object log10 extends BIF1(Vector(), "log10") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.log10(num.toDouble))
  }
}
object cos extends BIF1(Vector(), "cos") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.cos(num.toDouble))
  }
}
object exp extends BIF1(Vector(), "exp") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.exp(num.toDouble))
  }
}
object cbrt extends BIF1(Vector(), "cbrt") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.cbrt(num.toDouble))
  }
}
object atan extends BIF1(Vector(), "atan") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.atan(num.toDouble))
  }
}
object ceil extends BIF1(Vector(), "ceil") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.ceil(num.toDouble))
  }
}
object rint extends BIF1(Vector(), "rint") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.rint(num.toDouble))
  }
}
object log1p extends BIF1(Vector(), "log1p") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.log1p(num.toDouble))
  }
}
object sqrt extends BIF1(Vector(), "sqrt") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.sqrt(num.toDouble))
  }
}
object floor extends BIF1(Vector(), "floor") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.floor(num.toDouble))
  }
}
object toRadians extends BIF1(Vector(), "toRadians") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.toRadians(num.toDouble))
  }
}
object tanh extends BIF1(Vector(), "tanh") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.tanh(num.toDouble))
  }
}
object round extends BIF1(Vector(), "round") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.round(num.toDouble))
  }
}
object cosh extends BIF1(Vector(), "cosh") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.cosh(num.toDouble))
  }
}
object tan extends BIF1(Vector(), "tan") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.tan(num.toDouble))
  }
}
object abs extends BIF1(Vector(), "abs") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.abs(num.toDouble))
  }
}
object sin extends BIF1(Vector(), "sin") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.sin(num.toDouble))
  }
}
object nextUp extends BIF1(Vector(), "nextUp") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.nextUp(num.toDouble))
  }
}
object log extends BIF1(Vector(), "log") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.log(num.toDouble))
  }
}
object signum extends BIF1(Vector(), "signum") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.signum(num.toDouble))
  }
}
object acos extends BIF1(Vector(), "acos") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.acos(num.toDouble))
  }
}
object ulp extends BIF1(Vector(), "ulp") {
  val operandType = Some(SDecimal)
  val operation: PartialFunction[SValue, SValue] = {
    case SDecimal(num) => SDecimal(Math.ulp(num.toDouble))
  }
}}
