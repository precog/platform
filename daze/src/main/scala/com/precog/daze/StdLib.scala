package com.precog
package daze

import memoization._

import bytecode.Library
import bytecode.BuiltInRed
import bytecode.BuiltInFunc1
import bytecode.BuiltInFunc2

import yggdrasil._

trait GenOpcode extends ImplLibrary {
  private val defaultReductionOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
  abstract class BIR(val namespace: Vector[String], val name: String, val opcode: Int = defaultReductionOpcode.getAndIncrement) extends ReductionImpl 

  private val defaultUnaryOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
  abstract class BIF1(val namespace: Vector[String], val name: String, val opcode: Int = defaultUnaryOpcode.getAndIncrement) extends BuiltInFunc1Impl 

  private val defaultBinaryOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
  abstract class BIF2(val namespace: Vector[String], val name: String, val opcode: Int = defaultBinaryOpcode.getAndIncrement) extends BuiltInFunc2Impl
}

trait ImplLibrary extends Library {
  type Dataset[E]
  type DepGraph
  type Context

  lazy val libReduct = _libReduct
  lazy val lib1 = _lib1
  lazy val lib2 = _lib2

  def _libReduct: Set[BIR] = Set()
  def _lib1: Set[BIF1] = Set()
  def _lib2: Set[BIF2] = Set()

  trait ReductionImpl extends BuiltInRed {
    def reduced(enum: Dataset[SValue], graph: DepGraph, ctx: Context): Option[SValue]
  }

  trait BuiltInFunc1Impl extends BuiltInFunc1 {
    val operation: PartialFunction[SValue, SValue]
    val operandType: Option[SType]

    def evalEnum(enum: Dataset[SValue], graph: DepGraph, ctx: Context): Option[Dataset[SValue]] = None
  }

  trait BuiltInFunc2Impl extends BuiltInFunc2 {
    val operation: PartialFunction[(SValue, SValue), SValue]
    val operandType: (Option[SType], Option[SType])
    
    val requiresReduction: Boolean = false
    def reduced(enum: Dataset[SValue]): Option[SValue] = None
  }

  type BIR <: ReductionImpl
  type BIF1 <: BuiltInFunc1Impl
  type BIF2 <: BuiltInFunc2Impl
}

trait StdLib extends InfixLib with ReductionLib with TimeLib with MathLib with StringLib with StatsLib 


