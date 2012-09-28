package com.precog
package daze

import bytecode.{ BinaryOperationType, JNumberT, JBooleanT, JTextT, Library, Instructions }

import yggdrasil._
import yggdrasil.table._

import com.precog.util.NumericComparisons

trait InfixLib[M[+_]] extends GenOpcode[M] with Instructions {

  import StdLib.{BoolFrom, DoubleFrom, LongFrom, NumFrom, StrFrom}
  
  def PrimitiveEqualsF2 = yggdrasil.table.cf.std.Eq
  
  def op2ForBinOp(op: instructions.BinaryOperation): Option[Op2] = {
    import instructions._
    
    op match {
      case BuiltInFunction2Op(op2) => Some(op2)
      
      case instructions.Add => Some(Infix.Add)
      case instructions.Sub => Some(Infix.Sub)
      case instructions.Mul => Some(Infix.Mul)
      case instructions.Div => Some(Infix.Div)
      case instructions.Mod => Some(Infix.Mod)
      
      case instructions.Lt => Some(Infix.Lt)
      case instructions.LtEq => Some(Infix.LtEq)
      case instructions.Gt => Some(Infix.Gt)
      case instructions.GtEq => Some(Infix.GtEq)
      
      case instructions.Eq | instructions.NotEq => None
      
      case instructions.Or => Some(Infix.Or)
      case instructions.And => Some(Infix.And)
      
      case instructions.WrapObject | instructions.JoinObject |
      instructions.JoinArray | instructions.ArraySwap |
      instructions.DerefObject | instructions.DerefArray => None
    }
  }
  
  object Infix {
    val InfixNamespace = Vector("std", "infix")

    final def longOk(x: Long, y: Long) = true
    final def doubleOk(x: Double, y: Double) = true
    final def numOk(x: BigDecimal, y: BigDecimal) = true

    final def longNeZero(x: Long, y: Long) = y != 0
    final def doubleNeZero(x: Double, y: Double) = y != 0.0
    final def numNeZero(x: BigDecimal, y: BigDecimal) = y != 0

    class InfixOp2(name: String, longf: (Long, Long) => Long,
      doublef: (Double, Double) => Double,
      numf: (BigDecimal, BigDecimal) => BigDecimal)
    extends Op2(InfixNamespace, name) {
      val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)
      def f2: F2 = new CF2P({
        case (c1: LongColumn, c2: LongColumn) =>
          new LongFrom.LL(c1, c2, longOk, longf)

        case (c1: LongColumn, c2: DoubleColumn) =>
          new NumFrom.LD(c1, c2, numOk, numf)

        case (c1: LongColumn, c2: NumColumn) =>
          new NumFrom.LN(c1, c2, numOk, numf)

        case (c1: DoubleColumn, c2: LongColumn) =>
          new NumFrom.DL(c1, c2, numOk, numf)

        case (c1: DoubleColumn, c2: DoubleColumn) =>
          new DoubleFrom.DD(c1, c2, doubleOk, doublef)

        case (c1: DoubleColumn, c2: NumColumn) =>
          new NumFrom.DN(c1, c2, numOk, numf)

        case (c1: NumColumn, c2: LongColumn) =>
          new NumFrom.NL(c1, c2, numOk, numf)

        case (c1: NumColumn, c2: DoubleColumn) =>
          new NumFrom.ND(c1, c2, numOk, numf)

        case (c1: NumColumn, c2: NumColumn) =>
          new NumFrom.NN(c1, c2, numOk, numf)
      })
    }

    val Add = new InfixOp2("add", _ + _, _ + _, _ + _)
    val Sub = new InfixOp2("subtract", _ - _, _ - _, _ - _)
    val Mul = new InfixOp2("multiply", _ * _, _ * _, _ * _)

    // div needs to make sure to use Double even for division with longs
    val Div = new Op2(InfixNamespace, "divide") {
      def doublef(x: Double, y: Double) = x / y
      def numf(x: BigDecimal, y: BigDecimal) = x / y
      val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)
      def f2: F2 = new CF2P({
        case (c1: LongColumn, c2: LongColumn) =>
          new DoubleFrom.LL(c1, c2, doubleNeZero, doublef)

        case (c1: LongColumn, c2: DoubleColumn) =>
          new NumFrom.LD(c1, c2, numNeZero, numf)

        case (c1: LongColumn, c2: NumColumn) =>
          new NumFrom.LN(c1, c2, numNeZero, numf)

        case (c1: DoubleColumn, c2: LongColumn) =>
          new NumFrom.DL(c1, c2, numNeZero, numf)

        case (c1: DoubleColumn, c2: DoubleColumn) =>
          new DoubleFrom.DD(c1, c2, doubleNeZero, doublef)

        case (c1: DoubleColumn, c2: NumColumn) =>
          new NumFrom.DN(c1, c2, numNeZero, numf)

        case (c1: NumColumn, c2: LongColumn) =>
          new NumFrom.NL(c1, c2, numNeZero, numf)

        case (c1: NumColumn, c2: DoubleColumn) =>
          new NumFrom.ND(c1, c2, numNeZero, numf)

        case (c1: NumColumn, c2: NumColumn) =>
          new NumFrom.NN(c1, c2, numNeZero, numf)
      })
    }

    val Mod = new Op2(InfixNamespace, "mod") {
      val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)

      def longMod(x: Long, y: Long) = if ((x ^ y) < 0) (x % y) + y else x % y

      def doubleMod(x: Double, y: Double) =
        if (x.signum * y.signum == -1) x % y + y else x % y

      def numMod(x: BigDecimal, y: BigDecimal) =
        if (x.signum * y.signum == -1) x % y + y else x % y

      def f2: F2 = new CF2P({
        case (c1: LongColumn, c2: LongColumn) =>
          new LongFrom.LL(c1, c2, longNeZero, longMod)

        case (c1: LongColumn, c2: DoubleColumn) =>
          new NumFrom.LD(c1, c2, numNeZero, numMod)

        case (c1: LongColumn, c2: NumColumn) =>
          new NumFrom.LN(c1, c2, numNeZero, numMod)

        case (c1: DoubleColumn, c2: LongColumn) =>
          new NumFrom.DL(c1, c2, numNeZero, numMod)

        case (c1: DoubleColumn, c2: DoubleColumn) =>
          new DoubleFrom.DD(c1, c2, doubleNeZero, doubleMod)

        case (c1: DoubleColumn, c2: NumColumn) =>
          new NumFrom.DN(c1, c2, numNeZero, numMod)

        case (c1: NumColumn, c2: LongColumn) =>
          new NumFrom.NL(c1, c2, numNeZero, numMod)

        case (c1: NumColumn, c2: DoubleColumn) =>
          new NumFrom.ND(c1, c2, numNeZero, numMod)

        case (c1: NumColumn, c2: NumColumn) =>
          new NumFrom.NN(c1, c2, numNeZero, numMod)
      })
    }

    class CompareOp2(name: String, f: Int => Boolean)
    extends Op2(InfixNamespace, name) {
      val tpe = BinaryOperationType(JNumberT, JNumberT, JBooleanT)
      import NumericComparisons.compare
      def f2: F2 = new CF2P({
        case (c1: LongColumn, c2: LongColumn) =>
          new BoolFrom.LL(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))

        case (c1: LongColumn, c2: DoubleColumn) =>
          new BoolFrom.LD(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))

        case (c1: LongColumn, c2: NumColumn) =>
          new BoolFrom.LN(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))

        case (c1: DoubleColumn, c2: LongColumn) =>
          new BoolFrom.DL(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))

        case (c1: DoubleColumn, c2: DoubleColumn) =>
          new BoolFrom.DD(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))

        case (c1: DoubleColumn, c2: NumColumn) =>
          new BoolFrom.DN(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))

        case (c1: NumColumn, c2: LongColumn) =>
          new BoolFrom.NL(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))

        case (c1: NumColumn, c2: DoubleColumn) =>
          new BoolFrom.ND(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))

        case (c1: NumColumn, c2: NumColumn) =>
          new BoolFrom.NN(c1, c2, (x, y) => true, (x, y) => f(compare(x, y)))
      })
    }

    val Lt = new CompareOp2("lt", _ < 0)
    val LtEq = new CompareOp2("lte", _ <= 0)
    val Gt = new CompareOp2("gt", _ > 0)
    val GtEq = new CompareOp2("gte", _ >= 0)

    class BoolOp2(name: String, f: (Boolean, Boolean) => Boolean)
    extends Op2(InfixNamespace, name) {
      val tpe = BinaryOperationType(JBooleanT, JBooleanT, JBooleanT)
      def f2: F2 = new CF2P({
        case (c1: BoolColumn, c2: BoolColumn) => new BoolFrom.BB(c1, c2, f)
      })
    }

    val And = new BoolOp2("and", _ && _)
    val Or = new BoolOp2("or", _ || _)
    
    val concatString = new Op2(InfixNamespace, "concatString") {
      val tpe = BinaryOperationType(JTextT, JTextT, JTextT)
      def f2: F2 = new CF2P({
        case (c1: StrColumn, c2: StrColumn) =>
          new StrFrom.SS(c1, c2, _ != null && _ != null, _ + _)
      })
    }
  }
}
