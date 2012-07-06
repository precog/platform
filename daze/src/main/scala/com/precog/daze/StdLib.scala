package com.precog
package daze

import bytecode.Library
import bytecode.Arity
import bytecode.MorphismLike
import bytecode.Op1Like
import bytecode.Op2Like
import bytecode.ReductionLike

import yggdrasil._
import yggdrasil.table._

import scalaz.Monoid

trait GenOpcode extends ImplLibrary {
  private val defaultMorphismOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
  abstract class Morphism(val namespace: Vector[String], val name: String, val arity: Arity, val opcode: Int = defaultMorphismOpcode.getAndIncrement) extends MorphismImpl 

  private val defaultUnaryOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
  abstract class Op1(val namespace: Vector[String], val name: String, val opcode: Int = defaultUnaryOpcode.getAndIncrement) extends Op1Impl

  private val defaultBinaryOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
  abstract class Op2(val namespace: Vector[String], val name: String, val opcode: Int = defaultBinaryOpcode.getAndIncrement) extends Op2Impl

  private val defaultReductionOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
  abstract class Reduction(val namespace: Vector[String], val name: String, val opcode: Int = defaultReductionOpcode.getAndIncrement) extends ReductionImpl
}

trait ImplLibrary extends Library with ColumnarTableModule {
  lazy val libMorphism = _libMorphism
  lazy val lib1 = _lib1
  lazy val lib2 = _lib2
  lazy val libReduction = _libReduction

  def _libMorphism: Set[Morphism] = Set()
  def _lib1: Set[Op1] = Set()
  def _lib2: Set[Op2] = Set()
  def _libReduction: Set[Reduction] = Set()

  trait MorphismImpl extends MorphismLike {
    def alignment: Option[MorphismAlignment]  //not necessary for unary operations
    def apply(input: Table): Table
  }
  
  sealed trait MorphismAlignment
  
  object MorphismAlignment {
    case object Match extends MorphismAlignment
    case object Cross extends MorphismAlignment
  }

  trait Op1Impl extends Op1Like with MorphismImpl {
    lazy val alignment = None
    lazy val arity = Arity.One
    def apply(table: Table) = sys.error("morphism application of an op1")     // TODO make this actually work
    def f1: F1
  }

  trait Op2Impl extends Op2Like with MorphismImpl {
    lazy val alignment = None
    lazy val arity = Arity.Two
    def apply(table: Table) = sys.error("morphism application of an op2")     // TODO make this actually work
    def f2: F2
  }

  trait ReductionImpl extends ReductionLike with MorphismImpl {
    type Result
    lazy val alignment = None
    lazy val arity = Arity.One
    def apply(table: Table) = sys.error("morphism application of a reduction")     // TODO make this actually work
    def reducer: CReducer[Result]
    def monoid: Monoid[Result]
  }

  type Morphism <: MorphismImpl  //todo Morphism need to know eventually for Emitter if it's a unary or binary morphism
  type Op1 <: Op1Impl
  type Op2 <: Op2Impl
  type Reduction <: ReductionImpl
}

trait StdLib extends InfixLib with ReductionLib with TimeLib with MathLib with StringLib with StatsLib 


