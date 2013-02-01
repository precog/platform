package com.precog
package daze

import bytecode.{ BinaryOperationType, UnaryOperationType, JNumberT }
import bytecode.Library

import yggdrasil._
import yggdrasil.table._
import TransSpecModule._

trait MathLibModule[M[+_]] extends ColumnarTableLibModule[M] {
  trait MathLib extends ColumnarTableLib {
    import trans._
  
    val MathNamespace = Vector("std", "math")
  
    override def _lib1 = super._lib1 ++ Set(sinh, toDegrees, expm1, getExponent, asin, log10, cos, exp, cbrt, atan, ceil, rint, log1p, sqrt, floor, toRadians, tanh, round, cosh, tan, abs, sin, log, signum, acos, ulp)
  
    override def _lib2 = super._lib2 ++ Set(minOf, min, hypot, pow, maxOf, max, atan2, copySign, roundTo, IEEEremainder)
  
    import StdLib.{DoubleFrom, doubleIsDefined}
    import java.lang.Math
  
    abstract class Op1DD(name: String, defined: Double => Boolean, f: Double => Double)
    extends Op1F1(MathNamespace, name) {
      val tpe = UnaryOperationType(JNumberT, JNumberT)
      def f1(ctx: EvaluationContext): F1 = CF1P("builtin::math::op1dd::" + name) {
       case c: DoubleColumn => new DoubleFrom.D(c, defined, f)
       case c: LongColumn => new DoubleFrom.L(c, defined, f)
       case c: NumColumn => new DoubleFrom.N(c, defined, f)
      }
      def spec[A <: SourceType](ctx: EvaluationContext): TransSpec[A] => TransSpec[A] =
        transSpec => trans.Map1(transSpec, f1(ctx))
    }
  
    object sinh extends Op1DD("sinh", doubleIsDefined, Math.sinh)
  
    object toDegrees extends Op1DD("toDegrees", doubleIsDefined, Math.toDegrees)
  
    object expm1 extends Op1DD("expm1", doubleIsDefined, Math.expm1)
  
    object getExponent extends Op1DD("getExponent",
      n => doubleIsDefined(n) && n > 0.0, n => Math.getExponent(n).toDouble)
  
    object asin extends Op1DD("asin", n => -1.0 <= n && n <= 1.0, Math.asin)
  
    object log10 extends Op1DD("log10",
      n => doubleIsDefined(n) && n > 0.0, Math.log10)
  
    object cos extends Op1DD("cos", doubleIsDefined, Math.cos)
  
    object exp extends Op1DD("exp", doubleIsDefined, Math.exp)
  
    object cbrt extends Op1DD("cbrt", doubleIsDefined, Math.cbrt)
  
    object atan extends Op1DD("atan", doubleIsDefined, Math.atan)
  
    object ceil extends Op1DD("ceil", doubleIsDefined, Math.ceil)
  
    object rint extends Op1DD("rint", doubleIsDefined, Math.rint)
  
    object log1p extends Op1DD("log1p",
      n => doubleIsDefined(n) && n > -1.0, Math.log1p)
  
    object sqrt extends Op1DD("sqrt",
      n => doubleIsDefined(n) && n >= 0.0, Math.sqrt)
  
    object floor extends Op1DD("floor", doubleIsDefined, Math.floor)
  
    object toRadians extends Op1DD("toRadians", doubleIsDefined, Math.toRadians)
  
    object tanh extends Op1DD("tanh", doubleIsDefined, Math.tanh)
  
    // Math.round returns Long, so we have to improvise.
    // 4503599627370496.0 is the point where Double can't represent fractional
    // values anymore, so beyond that we just pass the value through
    object round extends Op1DD("round", doubleIsDefined,
      n => if (Math.abs(n) >= 4503599627370496.0) n else Math.round(n))
  
    object cosh extends Op1DD("cosh", doubleIsDefined, Math.cosh)
  
    object tan extends Op1DD("tan", doubleIsDefined, Math.tan)
  
    object abs extends Op1DD("abs", doubleIsDefined, Math.abs)
  
    object sin extends Op1DD("sin", doubleIsDefined, Math.sin)
  
    object log extends Op1DD("log", n => doubleIsDefined(n) && n > 0.0, Math.log)
  
    object signum extends Op1DD("signum", doubleIsDefined, Math.signum)
  
    object acos extends Op1DD("acos",
      n => doubleIsDefined(n) && -1.0 <= n && n <= 1.0, Math.acos)
  
    object ulp extends Op1DD("ulp", doubleIsDefined, Math.ulp)
  
    abstract class Op2DDD(name: String, defined:(Double, Double) => Boolean,
      f:(Double, Double) => Double) extends Op2F2(MathNamespace, name) {
      val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)
      def f2(ctx: EvaluationContext): F2 = CF2P("builtin::math::op2dd::" + name) {
        case (c1: DoubleColumn, c2: DoubleColumn) =>
            new DoubleFrom.DD(c1, c2, defined, f)
  
        case (c1: DoubleColumn, c2: LongColumn) =>
          new DoubleFrom.DL(c1, c2, defined, f)
  
        case (c1: DoubleColumn, c2: NumColumn) =>
          new DoubleFrom.DN(c1, c2, defined, f)
  
        case (c1: LongColumn, c2: DoubleColumn) =>
          new DoubleFrom.LD(c1, c2, defined, f)
  
        case (c1: NumColumn, c2: DoubleColumn) =>
          new DoubleFrom.ND(c1, c2, defined, f)
  
        case (c1: LongColumn, c2: LongColumn) =>
          new DoubleFrom.LL(c1, c2, defined, f)
  
        case (c1: LongColumn, c2: NumColumn) =>
          new DoubleFrom.LN(c1, c2, defined, f)
  
        case (c1: NumColumn, c2: LongColumn) =>
          new DoubleFrom.NL(c1, c2, defined, f)
  
        case (c1: NumColumn, c2: NumColumn) =>
          new DoubleFrom.NN(c1, c2, defined, f)
      }
    }
  
    def bothDefined(x: Double, y: Double) = doubleIsDefined(x) && doubleIsDefined(y)
  
    object minOf extends Op2DDD("minOf", bothDefined, Math.min)
  
    @deprecated
    object min extends Op2DDD("min", bothDefined, Math.min)
  
    object hypot extends Op2DDD("hypot", bothDefined, Math.hypot)
  
    object pow extends Op2DDD("pow", bothDefined, Math.pow)
  
    object maxOf extends Op2DDD("maxOf", bothDefined, Math.max)
  
    @deprecated
    object max extends Op2DDD("max", bothDefined, Math.max)
  
    object atan2 extends Op2DDD("atan2", bothDefined, Math.atan2)
  
    object copySign extends Op2DDD("copySign", bothDefined, Math.copySign)
  
    object IEEEremainder extends Op2DDD("IEEEremainder", bothDefined,
      Math.IEEEremainder)
  
    object roundTo extends Op2DDD("roundTo", bothDefined, { (n, digits) =>
      val adjusted = n * math.pow(10, digits)
      val rounded = if (Math.abs(n) >= 4503599627370496.0) adjusted else Math.round(adjusted)
      rounded * math.pow(10, -digits)
    })
  }
}

