package com.precog
package daze

import bytecode._
import bytecode.Library
import bytecode.Morphism1Like
import bytecode.Morphism2Like
import bytecode.Op1Like
import bytecode.Op2Like
import bytecode.ReductionLike

import common.json._

import yggdrasil._
import yggdrasil.table._

import blueeyes.json._

import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.apply._
import scala.annotation.tailrec

trait TableLibModule[M[+_]] extends TableModule[M] with TransSpecModule {
  type Lib <: TableLib

  object TableLib {
    private val defaultMorphism1Opcode = new java.util.concurrent.atomic.AtomicInteger(0)
    private val defaultMorphism2Opcode = new java.util.concurrent.atomic.AtomicInteger(0)
    private val defaultUnaryOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
    private val defaultBinaryOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
    private val defaultReductionOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
  }

  trait TableLib extends Library {
    import TableLib._
    import trans._

    lazy val libMorphism1 = _libMorphism1
    lazy val libMorphism2 = _libMorphism2
    lazy val lib1 = _lib1
    lazy val lib2 = _lib2
    lazy val libReduction = _libReduction

    def _libMorphism1: Set[Morphism1] = Set()
    def _libMorphism2: Set[Morphism2] = Set()
    def _lib1: Set[Op1] = Set()
    def _lib2: Set[Op2] = Set()
    def _libReduction: Set[Reduction] = Set()

    sealed trait MorphismAlignment 
    object MorphismAlignment {
      case object Match extends MorphismAlignment
      case object Cross extends MorphismAlignment
    }

    abstract class Morphism1(val namespace: Vector[String], val name: String, val opcode: Int = defaultMorphism1Opcode.getAndIncrement) extends Morphism1Like {
      def apply(input: Table, ctx: EvaluationContext): M[Table]
    }
    
    abstract class Morphism2(val namespace: Vector[String], val name: String, val opcode: Int = defaultMorphism1Opcode.getAndIncrement) extends Morphism2Like {
      def alignment: MorphismAlignment
      val multivariate: Boolean = false
      def apply(input: Table, ctx: EvaluationContext): M[Table]
    }
   
    abstract class Op1(namespace: Vector[String], name: String, opcode: Int = defaultUnaryOpcode.getAndIncrement) extends Morphism1(namespace, name, opcode) with Op1Like {
      def apply(table: Table, ctx: EvaluationContext) = sys.error("morphism application of an op1 is wrong")
      def spec[A <: SourceType](ctx: EvaluationContext): TransSpec[A] => TransSpec[A]

      def fold[A](op1: Op1 => A, op1F1: Op1F1 => A): A = op1(this)
    }

    abstract class Op1F1(namespace: Vector[String], name: String, opcode: Int = defaultUnaryOpcode.getAndIncrement) extends Op1(namespace, name, opcode) {
      def f1(ctx: EvaluationContext): F1

      override def fold[A](op1: Op1 => A, op1F1: Op1F1 => A): A = op1F1(this)
    }

    abstract class Op2(val namespace: Vector[String], val name: String, val opcode: Int = defaultBinaryOpcode.getAndIncrement) extends Op2Like {
      val alignment = MorphismAlignment.Match // Was None, which would have blown up in the evaluator
      def apply(table: Table, ctx: EvaluationContext) = sys.error("morphism application of an op2 is wrong")
      def f2(ctx: EvaluationContext): F2
    }

    abstract class Reduction(val namespace: Vector[String], val name: String, val opcode: Int = defaultReductionOpcode.getAndIncrement) extends ReductionLike {
      type Result
      def apply(table: Table, ctx: EvaluationContext) = table.reduce(reducer(ctx))(monoid) map extract

      def monoid: Monoid[Result]
      def reducer(ctx: EvaluationContext): Reducer[Result]
      def extract(res: Result): Table
      def extractValue(res: Result): Option[CValue]
    }

    def coalesce(reductions: List[(Reduction, Option[Int])]): Reduction
  }
}

trait ColumnarTableLibModule[M[+_]] extends TableLibModule[M] with ColumnarTableModule[M] {
  trait ColumnarTableLib extends TableLib {
    class WrapArrayTableReduction(val r: Reduction, val idx: Option[Int]) extends Reduction(r.namespace, r.name) {
      type Result = r.Result
      val tpe = r.tpe

      def monoid = r.monoid
      def reducer(ctx: EvaluationContext) = new CReducer[Result] {
        def reduce(cols: JType => Set[Column], range: Range): Result = {
          idx match {
            case Some(jdx) =>
              val cols0 = (tpe: JType) => cols(JArrayFixedT(Map(jdx -> tpe)))
              r.reducer(ctx).reduce(cols0, range)
            case None => 
              r.reducer(ctx).reduce(cols, range)
          }
        }
      }

      def extract(res: Result): Table = {
        r.extract(res).transform(trans.WrapArray(trans.Leaf(trans.Source)))
      }

      def extractValue(res: Result) = r.extractValue(res)
    }

    def coalesce(reductions: List[(Reduction, Option[Int])]): Reduction = {
      def rec(reductions: List[(Reduction, Option[Int])], acc: Reduction): Reduction = {
        reductions match {
          case (x, idx) :: xs =>
            val impl = new Reduction(Vector(), "") {
              type Result = (x.Result, acc.Result) 

              def reducer(ctx: EvaluationContext) = new CReducer[Result] {
                def reduce(cols: JType => Set[Column], range: Range): Result = {
                  idx match {
                    case Some(jdx) =>
                      val cols0 = (tpe: JType) => cols(JArrayFixedT(Map(jdx -> tpe)))
                      val (a, b) = (x.reducer(ctx).reduce(cols0, range), acc.reducer(ctx).reduce(cols, range))
                      (a, b)
                    case None => 
                      (x.reducer(ctx).reduce(cols, range), acc.reducer(ctx).reduce(cols, range))
                  }
                }
              }

              implicit val monoid: Monoid[Result] = new Monoid[Result] {
                def zero = (x.monoid.zero, acc.monoid.zero)
                def append(r1: Result, r2: => Result): Result = {
                  (x.monoid.append(r1._1, r2._1), acc.monoid.append(r1._2, r2._2))
                }
              }

              def extract(r: Result): Table = {
                import trans._
                
                val left = x.extract(r._1)
                val right = acc.extract(r._2)
                
                left.cross(right)(OuterArrayConcat(WrapArray(Leaf(SourceLeft)), Leaf(SourceRight)))
              }

              // TODO: Can't translate this into a CValue. Evaluator
              // won't inline the results. See call to inlineNodeValue
              def extractValue(res: Result) = None

              val tpe = UnaryOperationType(
                JUnionT(x.tpe.arg, acc.tpe.arg), 
                JArrayUnfixedT
              ) 
            }

            rec(xs, impl)

          case Nil => acc
        }
      }
      
      val (impl1, idx1) = reductions.head
      rec(reductions.tail, new WrapArrayTableReduction(impl1, idx1))
    }
  }
}

trait StdLibModule[M[+_]] 
    extends InfixLibModule[M]
    with UnaryLibModule[M]
    with ReductionLibModule[M]
    with ArrayLibModule[M]
    with TimeLibModule[M]
    with MathLibModule[M]
    with TypeLibModule[M]
    with StringLibModule[M]
    with StatsLibModule[M]
    with LogisticRegressionLibModule[M]
    with LinearRegressionLibModule[M]
    with FSLibModule[M] {
  type Lib <: StdLib

  trait StdLib
      extends InfixLib
      with UnaryLib
      with ReductionLib
      with ArrayLib
      with TimeLib
      with MathLib
      with TypeLib
      with StringLib
      with StatsLib
      with LogisticRegressionLib
      with LinearRegressionLib
      with FSLib
}

//TODO: Move to ColumnSupport or some such place
object StdLib {
  import java.lang.Double.{isNaN, isInfinite}

  def doubleIsDefined(n: Double) = !(isNaN(n) || isInfinite(n))

  object StrFrom {
    class L(c: LongColumn, defined: Long => Boolean, f: Long => String)
        extends Map1Column(c)
        with StrColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row))

      def apply(row: Int) = f(c(row))
    }

    class D(c: DoubleColumn, defined: Double => Boolean, f: Double => String)
        extends Map1Column(c)
        with StrColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row))

      def apply(row: Int) = f(c(row))
    }

    class N(c: NumColumn, defined: BigDecimal => Boolean, f: BigDecimal => String)
        extends Map1Column(c)
        with StrColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row))

      def apply(row: Int) = f(c(row))
    }

    class S(c: StrColumn, defined: String => Boolean, f: String => String)
        extends Map1Column(c)
        with StrColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row))

      def apply(row: Int) = f(c(row))
    }

    class SS(
      c1: StrColumn, c2: StrColumn, defined: (String, String) => Boolean,
      f: (String, String) => String)
        extends Map2Column(c1, c2)
        with StrColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class SD(
      c1: StrColumn, c2: DoubleColumn, defined: (String, Double) => Boolean,
      f: (String, Double) => String)
        extends Map2Column(c1, c2)
        with StrColumn {

      override def isDefinedAt(row: Int) = super.isDefinedAt(row) &&
        c1(row) != null && doubleIsDefined(c2(row)) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class SL(
      c1: StrColumn, c2: LongColumn, defined: (String, Long) => Boolean,
      f: (String, Long) => String)
        extends Map2Column(c1, c2)
        with StrColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && c1(row) != null && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class SN(
      c1: StrColumn, c2: NumColumn, defined: (String, BigDecimal) => Boolean,
      f: (String, BigDecimal) => String)
        extends Map2Column(c1, c2)
        with StrColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && c1(row) != null && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }
  }

  object LongFrom {
    class L(c: LongColumn, defined: Long => Boolean, f: Long => Long)
        extends Map1Column(c)
        with LongColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row))

      def apply(row: Int) = f(c(row))
    }

    class S(c: StrColumn, defined: String => Boolean, f: String => Long)
        extends Map1Column(c)
        with LongColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row))

      def apply(row: Int) = f(c(row))
    }

    class LL(
      c1: LongColumn, c2: LongColumn, defined: (Long, Long) => Boolean,
      f: (Long, Long) => Long)
        extends Map2Column(c1, c2)
        with LongColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class SS(
      c1: StrColumn, c2: StrColumn, defined: (String, String) => Boolean,
      f: (String, String) => Long)
        extends Map2Column(c1, c2)
        with LongColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class SD(
      c1: StrColumn, c2: DoubleColumn, defined: (String, Double) => Boolean,
      f: (String, Double) => Long)
        extends Map2Column(c1, c2)
        with LongColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class SL(
      c1: StrColumn, c2: LongColumn, defined: (String, Long) => Boolean,
      f: (String, Long) => Long)
        extends Map2Column(c1, c2)
        with LongColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class SN(
      c1: StrColumn, c2: NumColumn, defined: (String, BigDecimal) => Boolean,
      f: (String, BigDecimal) => Long)
        extends Map2Column(c1, c2)
        with LongColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }
  }

  object DoubleFrom {
    class D(c: DoubleColumn, defined: Double => Boolean, f: Double => Double)
        extends Map1Column(c)
        with DoubleColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row)) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c(row))
    }

    class L(c: LongColumn, defined: Double => Boolean, f: Double => Double)
        extends Map1Column(c)
        with DoubleColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row).toDouble) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c(row))
    }

    class N(c: NumColumn, defined: Double => Boolean, f: Double => Double)
        extends Map1Column(c)
        with DoubleColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row).toDouble) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c(row).toDouble)
    }

    class DD(
      c1: DoubleColumn, c2: DoubleColumn, defined: (Double, Double) => Boolean,
      f: (Double, Double) => Double)
        extends Map2Column(c1, c2)
        with DoubleColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row)) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }
  
    class DL(
      c1: DoubleColumn, c2: LongColumn, defined: (Double, Double) => Boolean,
      f: (Double, Double) => Double)
        extends Map2Column(c1, c2)
        with DoubleColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row).toDouble) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c1(row), c2(row).toDouble)
    }
  
    class DN(
      c1: DoubleColumn, c2: NumColumn, defined: (Double, Double) => Boolean,
      f: (Double, Double) => Double)
        extends Map2Column(c1, c2)
        with DoubleColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row).toDouble) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c1(row), c2(row).toDouble)
    }
  
    class LD(
      c1: LongColumn, c2: DoubleColumn, defined: (Double, Double) => Boolean,
      f: (Double, Double) => Double)
        extends Map2Column(c1, c2)
        with DoubleColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row).toDouble, c2(row)) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c1(row).toDouble, c2(row))
    }
  
    class LL(
      c1: LongColumn, c2: LongColumn, defined: (Double, Double) => Boolean,
      f: (Double, Double) => Double)
        extends Map2Column(c1, c2)
        with DoubleColumn {

      override def isDefinedAt(row: Int) = super.isDefinedAt(row) &&
        defined(c1(row).toDouble, c2(row).toDouble) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c1(row).toDouble, c2(row).toDouble)
    }

    class LN(
      c1: LongColumn, c2: NumColumn, defined: (Double, Double) => Boolean,
      f: (Double, Double) => Double)
        extends Map2Column(c1, c2)
        with DoubleColumn {

      override def isDefinedAt(row: Int) = super.isDefinedAt(row) &&
        defined(c1(row).toDouble, c2(row).toDouble) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c1(row).toDouble, c2(row).toDouble)
    }
  
    class ND(
      c1: NumColumn, c2: DoubleColumn, defined: (Double, Double) => Boolean,
      f: (Double, Double) => Double)
        extends Map2Column(c1, c2)
        with DoubleColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row).toDouble, c2(row)) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c1(row).toDouble, c2(row))
    }
  
    class NL(
      c1: NumColumn, c2: LongColumn, defined: (Double, Double) => Boolean,
      f: (Double, Double) => Double)
        extends Map2Column(c1, c2)
        with DoubleColumn {

      override def isDefinedAt(row: Int) = super.isDefinedAt(row) &&
        defined(c1(row).toDouble, c2(row).toDouble) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c1(row).toDouble, c2(row).toDouble)
    }
  
    class NN(
      c1: NumColumn, c2: NumColumn, defined: (Double, Double) => Boolean,
      f: (Double, Double) => Double)
        extends Map2Column(c1, c2)
        with DoubleColumn {

      override def isDefinedAt(row: Int) = super.isDefinedAt(row) &&
        defined(c1(row).toDouble, c2(row).toDouble) && doubleIsDefined(apply(row))

      def apply(row: Int) = f(c1(row).toDouble, c2(row).toDouble)
    }
  }

  object NumFrom {
    class N(c: NumColumn, defined: BigDecimal => Boolean, f: BigDecimal => BigDecimal)
        extends Map1Column(c)
        with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row))

      def apply(row: Int) = f(c(row))
    }

    class DD(
      c1: DoubleColumn, c2: DoubleColumn,
      defined: (BigDecimal, BigDecimal) => Boolean,
      f: (BigDecimal, BigDecimal) => BigDecimal)
        extends Map2Column(c1, c2)
        with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(BigDecimal(c1(row)), BigDecimal(c2(row)))

      def apply(row: Int) = f(BigDecimal(c1(row)), BigDecimal(c2(row)))
    }
  
    class DL(
      c1: DoubleColumn, c2: LongColumn,
      defined: (BigDecimal, BigDecimal) => Boolean,
      f: (BigDecimal, BigDecimal) => BigDecimal)
        extends Map2Column(c1, c2)
        with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(BigDecimal(c1(row)), BigDecimal(c2(row)))

      def apply(row: Int) = f(BigDecimal(c1(row)), BigDecimal(c2(row)))
    }
  
    class DN(
      c1: DoubleColumn, c2: NumColumn,
      defined: (BigDecimal, BigDecimal) => Boolean,
      f: (BigDecimal, BigDecimal) => BigDecimal)
        extends Map2Column(c1, c2)
        with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(BigDecimal(c1(row)), c2(row))

      def apply(row: Int) = f(BigDecimal(c1(row)), c2(row))
    }
  
    class LD(
      c1: LongColumn, c2: DoubleColumn,
      defined: (BigDecimal, BigDecimal) => Boolean,
      f: (BigDecimal, BigDecimal) => BigDecimal)
        extends Map2Column(c1, c2)
        with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(BigDecimal(c1(row)), BigDecimal(c2(row)))

      def apply(row: Int) = f(BigDecimal(c1(row)), BigDecimal(c2(row)))
    }
  
    class LL(
      c1: LongColumn, c2: LongColumn,
      defined: (BigDecimal, BigDecimal) => Boolean,
      f: (BigDecimal, BigDecimal) => BigDecimal)
        extends Map2Column(c1, c2)
        with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(BigDecimal(c1(row)), BigDecimal(c2(row)))

      def apply(row: Int) = f(BigDecimal(c1(row)), BigDecimal(c2(row).toDouble))
    }
  
    class LN(
      c1: LongColumn, c2: NumColumn,
      defined: (BigDecimal, BigDecimal) => Boolean,
      f: (BigDecimal, BigDecimal) => BigDecimal)
        extends Map2Column(c1, c2)
        with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(BigDecimal(c1(row)), c2(row))

      def apply(row: Int) = f(BigDecimal(c1(row)), c2(row))
    }
  
    class ND(
      c1: NumColumn, c2: DoubleColumn,
      defined: (BigDecimal, BigDecimal) => Boolean,
      f: (BigDecimal, BigDecimal) => BigDecimal)
        extends Map2Column(c1, c2)
        with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), BigDecimal(c2(row)))

      def apply(row: Int) = f(c1(row), BigDecimal(c2(row)))
    }
  
    class NL(
      c1: NumColumn, c2: LongColumn,
      defined: (BigDecimal, BigDecimal) => Boolean,
      f: (BigDecimal, BigDecimal) => BigDecimal)
        extends Map2Column(c1, c2)
        with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), BigDecimal(c2(row)))

      def apply(row: Int) = f(c1(row), BigDecimal(c2(row)))
    }
  
    class NN(
      c1: NumColumn, c2: NumColumn,
      defined: (BigDecimal, BigDecimal) => Boolean,
      f: (BigDecimal, BigDecimal) => BigDecimal)
        extends Map2Column(c1, c2)
        with NumColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }
  }

  object BoolFrom {
    class B(c: BoolColumn, f: Boolean => Boolean)
        extends Map1Column(c)
        with BoolColumn {

      def apply(row: Int) = f(c(row))
    }

    class BB(c1: BoolColumn, c2: BoolColumn, f: (Boolean, Boolean) => Boolean)
        extends Map2Column(c1, c2)
        with BoolColumn {

      def apply(row: Int) = f(c1(row), c2(row))
    }

    class S(c: StrColumn, defined: String => Boolean, f: String => Boolean)
        extends Map1Column(c)
        with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c(row))

      def apply(row: Int) = f(c(row))
    }

    class SS(
      c1: StrColumn, c2: StrColumn, defined: (String, String) => Boolean,
      f: (String, String) => Boolean)
        extends Map2Column(c1, c2)
        with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }
  
    class DD(
      c1: DoubleColumn, c2: DoubleColumn, defined: (Double, Double) => Boolean,
      f: (Double, Double) => Boolean)
        extends Map2Column(c1, c2)
        with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }
  
    class DL(
      c1: DoubleColumn, c2: LongColumn, defined: (Double, Long) => Boolean,
      f: (Double, Long) => Boolean)
        extends Map2Column(c1, c2)
        with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }
  
    class DN(
      c1: DoubleColumn, c2: NumColumn,
      defined: (Double, BigDecimal) => Boolean,
      f: (Double, BigDecimal) => Boolean)
        extends Map2Column(c1, c2)
        with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }
  
    class LD(
      c1: LongColumn, c2: DoubleColumn, defined: (Long, Double) => Boolean,
      f: (Long, Double) => Boolean)
        extends Map2Column(c1, c2)
        with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }
  
    class LL(
      c1: LongColumn, c2: LongColumn, defined: (Long, Long) => Boolean,
      f: (Long, Long) => Boolean)
        extends Map2Column(c1, c2)
        with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }
  
    class LN(
      c1: LongColumn, c2: NumColumn, defined: (Long, BigDecimal) => Boolean,
      f: (Long, BigDecimal) => Boolean)
        extends Map2Column(c1, c2)
        with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }
  
    class ND(
      c1: NumColumn, c2: DoubleColumn,
      defined: (BigDecimal, Double) => Boolean,
      f: (BigDecimal, Double) => Boolean)
        extends Map2Column(c1, c2)
        with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }
  
    class NL(
      c1: NumColumn, c2: LongColumn, defined: (BigDecimal, Long) => Boolean,
      f: (BigDecimal, Long) => Boolean)
        extends Map2Column(c1, c2)
        with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }
  
    class NN(
      c1: NumColumn, c2: NumColumn,
      defined: (BigDecimal, BigDecimal) => Boolean,
      f: (BigDecimal, BigDecimal) => Boolean)
        extends Map2Column(c1, c2)
        with BoolColumn {

      override def isDefinedAt(row: Int) =
        super.isDefinedAt(row) && defined(c1(row), c2(row))

      def apply(row: Int) = f(c1(row), c2(row))
    }
  }
}
