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

import bytecode._
import bytecode.Library
import bytecode.Morphism1Like
import bytecode.Morphism2Like
import bytecode.Op1Like
import bytecode.Op2Like
import bytecode.ReductionLike

import yggdrasil._
import yggdrasil.table._

import blueeyes.json._

import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.apply._
import scala.annotation.tailrec

trait GenOpcode[M[+_]] extends ImplLibrary[M] {
  private val defaultMorphism1Opcode = new java.util.concurrent.atomic.AtomicInteger(0)
  abstract class Morphism1(val namespace: Vector[String], val name: String, val opcode: Int = defaultMorphism1Opcode.getAndIncrement) extends Morphism1Impl 

  private val defaultMorphism2Opcode = new java.util.concurrent.atomic.AtomicInteger(0)
  abstract class Morphism2(val namespace: Vector[String], val name: String, val opcode: Int = defaultMorphism1Opcode.getAndIncrement) extends Morphism2Impl 

  private val defaultUnaryOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
  abstract class Op1(val namespace: Vector[String], val name: String, val opcode: Int = defaultUnaryOpcode.getAndIncrement) extends Op1Impl

  private val defaultBinaryOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
  abstract class Op2(val namespace: Vector[String], val name: String, val opcode: Int = defaultBinaryOpcode.getAndIncrement) extends Op2Impl

  abstract class Reduction(val namespace: Vector[String], val name: String, val opcode: Int = GenOpcode.defaultReductionOpcode.getAndIncrement) extends ReductionImpl
}

object GenOpcode {
  val defaultReductionOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
}

trait ImplLibrary[M[+_]] extends Library with ColumnarTableModule[M] {
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

  trait Morphism1Impl extends Morphism1Like {
    def apply(input: Table): M[Table]
  }
  
  trait Morphism2Impl extends Morphism2Like {
    def alignment: MorphismAlignment
    def apply(input: Table): M[Table]
  }
 
  sealed trait MorphismAlignment
  
  object MorphismAlignment {
    case object Match extends MorphismAlignment
    case object Cross extends MorphismAlignment
  }

  trait Op1Impl extends Op1Like with Morphism1Impl {
    def apply(table: Table) = sys.error("morphism application of an op1")     // TODO make this actually work
    def f1: F1
  }

  trait Op2Impl extends Op2Like {
    lazy val alignment = MorphismAlignment.Match // Was None, which would have blown up in the evaluator
    def apply(table: Table) = sys.error("morphism application of an op2")     // TODO make this actually work
    def f2: F2
    lazy val fqn = if (namespace.isEmpty) name else namespace.mkString("", "::", "::") + name
  }

  trait ReductionImpl extends ReductionLike with Morphism1Impl {
    type Result
    def apply(table: Table) = table.reduce(reducer) map extract
    def reducer: CReducer[Result]
    implicit def monoid: Monoid[Result]
    def extract(res: Result): Table
  }

  class WrapArrayReductionImpl(val r: ReductionImpl) extends ReductionImpl {
    type Result = r.Result
    def reducer = r.reducer
    implicit def monoid = r.monoid
    def extract(res: Result): Table = {
      r.extract(res).transform(trans.WrapArray(trans.Leaf(trans.Source)))
    }

    val tpe = r.tpe
    val opcode = r.opcode
    val name = r.name
    val namespace = r.namespace
  }

  def coalesce(reductions: NonEmptyList[ReductionImpl]): ReductionImpl = {
    def rec(reductions: List[ReductionImpl], acc: ReductionImpl): ReductionImpl = {
      reductions match {
        case x :: xs =>
          new ReductionImpl {
            type Result = (x.Result, acc.Result) 
            val reducer = new CReducer[Result] {
              def reduce(cols: JType => Set[Column], range: Range): Result = {
                (x.reducer.reduce(cols, range), acc.reducer.reduce(cols, range))
              }
            }

            implicit val monoid: Monoid[Result] = new Monoid[Result] {
              def zero = (x.monoid.zero, acc.monoid.zero)
              def append(r1: Result, r2: => Result): Result = {
                (x.monoid.append(r1._1, r2._1), acc.monoid.append(r1._2, r2._2))
              }
            }

            def extract(r: Result): Table = {
              val left = x.extract(r._1)
              val right = acc.extract(r._2)
              Table(Apply[({ type λ[α] = StreamT[M, α] })#λ].zip.zip(left.slices, right.slices) map { 
                case (sl, sr) => new Slice { 
                  val size = sl.size max sr.size
                  val columns = {
                    (sl.columns map { case (ColumnRef(jpath, ctype), col) => (ColumnRef(JPathIndex(0) \ jpath, ctype) -> col) }) ++ 
                    (sr.columns collect { case (ColumnRef(JPath(JPathIndex(j), xs @ _*), ctype), col) => (ColumnRef(JPath(JPathIndex(j + 1) +: xs: _*), ctype) -> col) })
                  }
                }
              })
            }

            val namespace = Vector()
            val name = ""
            val opcode = GenOpcode.defaultReductionOpcode.getAndIncrement
            val tpe = UnaryOperationType(
              JUnionT(x.tpe.arg, acc.tpe.arg), 
              JArrayUnfixedT
            ) 
          }

        case Nil => acc
      }
    }

    rec(reductions.tail, new WrapArrayReductionImpl(reductions.head))
  }

  type Morphism1 <: Morphism1Impl
  type Morphism2 <: Morphism2Impl
  type Op1 <: Op1Impl
  type Op2 <: Op2Impl
  type Reduction <: ReductionImpl
}

trait StdLib[M[+_]] extends InfixLib[M] with ReductionLib[M] with TimeLib[M] with MathLib[M] with StringLib[M] with StatsLib[M] 

object StdLib {
  import java.lang.Double.{isNaN, isInfinite}
  def doubleIsDefined(n:Double) = !(isNaN(n) || isInfinite(n))

  object StrFrom {
    class S(c:StrColumn, defined:String => Boolean, f:String => String) extends
    Map1Column(c) with StrColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c(row))
      def apply(row:Int) = f(c(row))
    }

    class SS(c1:StrColumn, c2:StrColumn, defined:(String, String) => Boolean, f:(String, String) => String)
    extends Map2Column(c1, c2) with StrColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row), c2(row))
      def apply(row: Int) = f(c1(row), c2(row))
    }
  }

  object LongFrom {
    class L(c:LongColumn, defined:Long => Boolean, f:Long => Long)
    extends Map1Column(c) with LongColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c(row))
      def apply(row: Int) = f(c(row))
    }

    class S(c:StrColumn, defined:String => Boolean, f:String => Long)
    extends Map1Column(c) with LongColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c(row))
      def apply(row: Int) = f(c(row))
    }

    class LL(c1:LongColumn, c2:LongColumn, defined:(Long, Long) => Boolean, f:(Long, Long) => Long)
    extends Map2Column(c1, c2) with LongColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row), c2(row))
      def apply(row: Int) = f(c1(row), c2(row))
    }

    class SS(c1:StrColumn, c2:StrColumn, defined:(String, String) => Boolean, f:(String, String) => String)
    extends Map2Column(c1, c2) with StrColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row), c2(row))
      def apply(row: Int) = f(c1(row), c2(row))
    }
  }

  object DoubleFrom {
    class D(c:DoubleColumn, defined:Double => Boolean, f:Double => Double)
    extends Map1Column(c) with DoubleColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c(row)) && doubleIsDefined(apply(row))
      def apply(row: Int) = f(c(row))
    }

    class L(c:LongColumn, defined:Double => Boolean, f:Double => Double)
    extends Map1Column(c) with DoubleColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c(row).toDouble) && doubleIsDefined(apply(row))
      def apply(row: Int) = f(c(row))
    }

    class N(c:NumColumn, defined:Double => Boolean, f:Double => Double)
    extends Map1Column(c) with DoubleColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c(row).toDouble) && doubleIsDefined(apply(row))
      def apply(row: Int) = f(c(row).toDouble)
    }

    class DD(c1:DoubleColumn, c2:DoubleColumn, defined:(Double, Double) => Boolean, f:(Double, Double) => Double)
    extends Map2Column(c1, c2) with DoubleColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row), c2(row)) && doubleIsDefined(apply(row))
      def apply(row: Int) = f(c1(row), c2(row))
    }
  
    class DL(c1:DoubleColumn, c2:LongColumn, defined:(Double, Double) => Boolean, f:(Double, Double) => Double)
    extends Map2Column(c1, c2) with DoubleColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row), c2(row).toDouble) && doubleIsDefined(apply(row))
      def apply(row: Int) = f(c1(row), c2(row).toDouble)
    }
  
    class DN(c1:DoubleColumn, c2:NumColumn, defined:(Double, Double) => Boolean, f:(Double, Double) => Double)
    extends Map2Column(c1, c2) with DoubleColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row), c2(row).toDouble) && doubleIsDefined(apply(row))
      def apply(row: Int) = f(c1(row), c2(row).toDouble)
    }
  
    class LD(c1:LongColumn, c2:DoubleColumn, defined:(Double, Double) => Boolean, f:(Double, Double) => Double)
    extends Map2Column(c1, c2) with DoubleColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row).toDouble, c2(row)) && doubleIsDefined(apply(row))
      def apply(row: Int) = f(c1(row).toDouble, c2(row))
    }
  
    class LL(c1:LongColumn, c2:LongColumn, defined:(Double, Double) => Boolean, f:(Double, Double) => Double)
    extends Map2Column(c1, c2) with DoubleColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row).toDouble, c2(row).toDouble) && doubleIsDefined(apply(row))
      def apply(row: Int) = f(c1(row).toDouble, c2(row).toDouble)
    }

    class LN(c1:LongColumn, c2:NumColumn, defined:(Double, Double) => Boolean, f:(Double, Double) => Double)
    extends Map2Column(c1, c2) with DoubleColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row).toDouble, c2(row).toDouble) && doubleIsDefined(apply(row))
      def apply(row: Int) = f(c1(row).toDouble, c2(row).toDouble)
    }
  
    class ND(c1:NumColumn, c2:DoubleColumn, defined:(Double, Double) => Boolean, f:(Double, Double) => Double)
    extends Map2Column(c1, c2) with DoubleColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row).toDouble, c2(row)) && doubleIsDefined(apply(row))
      def apply(row: Int) = f(c1(row).toDouble, c2(row))
    }
  
    class NL(c1:NumColumn, c2:LongColumn, defined:(Double, Double) => Boolean, f:(Double, Double) => Double)
    extends Map2Column(c1, c2) with DoubleColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row).toDouble, c2(row).toDouble) && doubleIsDefined(apply(row))
      def apply(row: Int) = f(c1(row).toDouble, c2(row).toDouble)
    }
  
    class NN(c1:NumColumn, c2:NumColumn, defined:(Double, Double) => Boolean, f:(Double, Double) => Double)
    extends Map2Column(c1, c2) with DoubleColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row).toDouble, c2(row).toDouble) && doubleIsDefined(apply(row))
      def apply(row: Int) = f(c1(row).toDouble, c2(row).toDouble)
    }
  }

  object NumFrom {
    class N(c:NumColumn, defined:BigDecimal => Boolean, f:BigDecimal => BigDecimal)
    extends Map1Column(c) with NumColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c(row))
      def apply(row: Int) = f(c(row))
    }

    class DD(c1:DoubleColumn, c2:DoubleColumn, defined:(BigDecimal, BigDecimal) => Boolean, f:(BigDecimal, BigDecimal) => BigDecimal)
    extends Map2Column(c1, c2) with NumColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(BigDecimal(c1(row)), BigDecimal(c2(row)))
      def apply(row: Int) = f(BigDecimal(c1(row)), BigDecimal(c2(row)))
    }
  
    class DL(c1:DoubleColumn, c2:LongColumn, defined:(BigDecimal, BigDecimal) => Boolean, f:(BigDecimal, BigDecimal) => BigDecimal)
    extends Map2Column(c1, c2) with NumColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(BigDecimal(c1(row)), BigDecimal(c2(row)))
      def apply(row: Int) = f(BigDecimal(c1(row)), BigDecimal(c2(row)))
    }
  
    class DN(c1:DoubleColumn, c2:NumColumn, defined:(BigDecimal, BigDecimal) => Boolean, f:(BigDecimal, BigDecimal) => BigDecimal)
    extends Map2Column(c1, c2) with NumColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(BigDecimal(c1(row)), c2(row))
      def apply(row: Int) = f(BigDecimal(c1(row)), c2(row))
    }
  
    class LD(c1:LongColumn, c2:DoubleColumn, defined:(BigDecimal, BigDecimal) => Boolean, f:(BigDecimal, BigDecimal) => BigDecimal)
    extends Map2Column(c1, c2) with NumColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(BigDecimal(c1(row)), BigDecimal(c2(row)))
      def apply(row: Int) = f(BigDecimal(c1(row)), BigDecimal(c2(row)))
    }
  
    class LL(c1:LongColumn, c2:LongColumn, defined:(BigDecimal, BigDecimal) => Boolean, f:(BigDecimal, BigDecimal) => BigDecimal)
    extends Map2Column(c1, c2) with NumColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(BigDecimal(c1(row)), BigDecimal(c2(row)))
      def apply(row: Int) = f(BigDecimal(c1(row)), BigDecimal(c2(row).toDouble))
    }
  
    class LN(c1:LongColumn, c2:NumColumn, defined:(BigDecimal, BigDecimal) => Boolean, f:(BigDecimal, BigDecimal) => BigDecimal)
    extends Map2Column(c1, c2) with NumColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(BigDecimal(c1(row)), c2(row))
      def apply(row: Int) = f(BigDecimal(c1(row)), c2(row))
    }
  
    class ND(c1:NumColumn, c2:DoubleColumn, defined:(BigDecimal, BigDecimal) => Boolean, f:(BigDecimal, BigDecimal) => BigDecimal)
    extends Map2Column(c1, c2) with NumColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row), BigDecimal(c2(row)))
      def apply(row: Int) = f(c1(row), BigDecimal(c2(row)))
    }
  
    class NL(c1:NumColumn, c2:LongColumn, defined:(BigDecimal, BigDecimal) => Boolean, f:(BigDecimal, BigDecimal) => BigDecimal)
    extends Map2Column(c1, c2) with NumColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row), BigDecimal(c2(row)))
      def apply(row: Int) = f(c1(row), BigDecimal(c2(row)))
    }
  
    class NN(c1:NumColumn, c2:NumColumn, defined:(BigDecimal, BigDecimal) => Boolean, f:(BigDecimal, BigDecimal) => BigDecimal)
    extends Map2Column(c1, c2) with NumColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row), c2(row))
      def apply(row: Int) = f(c1(row), c2(row))
    }
  }

  object BoolFrom {
    type LongF = (Long, Long) => Boolean
    type DoubleF = (Double, Double) => Boolean
    type NumF = (BigDecimal, BigDecimal) => Boolean
    type BoolF = (Boolean, Boolean) => Boolean
    type StrF = (String, String) => Boolean

    class B(c:BoolColumn, f:Boolean => Boolean) extends Map1Column(c) with BoolColumn {
      def apply(row: Int) = f(c(row))
    }

    class BB(c1:BoolColumn, c2:BoolColumn, f:(Boolean, Boolean) => Boolean)
    extends Map2Column(c1, c2) with BoolColumn {
      def apply(row: Int) = f(c1(row), c2(row))
    }

    class S(c:StrColumn, defined:String => Boolean, f:String => Boolean)
    extends Map1Column(c) with BoolColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c(row))
      def apply(row: Int) = f(c(row))
    }

    class SS(c1:StrColumn, c2:StrColumn, defined:(String, String) => Boolean, f:(String, String) => Boolean)
    extends Map2Column(c1, c2) with BoolColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row), c2(row))
      def apply(row: Int) = f(c1(row), c2(row))
    }
  
    class DD(c1:DoubleColumn, c2:DoubleColumn, defined:(Double, Double) => Boolean, f:(Double, Double) => Boolean)
    extends Map2Column(c1, c2) with BoolColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row), c2(row))
      def apply(row: Int) = f(c1(row), c2(row))
    }
  
    class DL(c1:DoubleColumn, c2:LongColumn, defined:(Double, Long) => Boolean, f:(Double, Long) => Boolean)
    extends Map2Column(c1, c2) with BoolColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row), c2(row))
      def apply(row: Int) = f(c1(row), c2(row))
    }
  
    class DN(c1:DoubleColumn, c2:NumColumn, defined:(Double, BigDecimal) => Boolean, f:(Double, BigDecimal) => Boolean)
    extends Map2Column(c1, c2) with BoolColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row), c2(row))
      def apply(row: Int) = f(c1(row), c2(row))
    }
  
    class LD(c1:LongColumn, c2:DoubleColumn, defined:(Long, Double) => Boolean, f:(Long, Double) => Boolean)
    extends Map2Column(c1, c2) with BoolColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row), c2(row))
      def apply(row: Int) = f(c1(row), c2(row))
    }
  
    class LL(c1:LongColumn, c2:LongColumn, defined:(Long, Long) => Boolean, f:(Long, Long) => Boolean)
    extends Map2Column(c1, c2) with BoolColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row), c2(row))
      def apply(row: Int) = f(c1(row), c2(row))
    }
  
    class LN(c1:LongColumn, c2:NumColumn, defined:(Long, BigDecimal) => Boolean, f:(Long, BigDecimal) => Boolean)
    extends Map2Column(c1, c2) with BoolColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row), c2(row))
      def apply(row: Int) = f(c1(row), c2(row))
    }
  
    class ND(c1:NumColumn, c2:DoubleColumn, defined:(BigDecimal, Double) => Boolean, f:(BigDecimal, Double) => Boolean)
    extends Map2Column(c1, c2) with BoolColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row), c2(row))
      def apply(row: Int) = f(c1(row), c2(row))
    }
  
    class NL(c1:NumColumn, c2:LongColumn, defined:(BigDecimal, Long) => Boolean, f:(BigDecimal, Long) => Boolean)
    extends Map2Column(c1, c2) with BoolColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row), c2(row))
      def apply(row: Int) = f(c1(row), c2(row))
    }
  
    class NN(c1:NumColumn, c2:NumColumn, defined:(BigDecimal, BigDecimal) => Boolean, f:(BigDecimal, BigDecimal) => Boolean)
    extends Map2Column(c1, c2) with BoolColumn {
      override def isDefinedAt(row:Int) = super.isDefinedAt(row) && defined(c1(row), c2(row))
      def apply(row: Int) = f(c1(row), c2(row))
    }
  }
}
