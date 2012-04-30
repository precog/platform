package com.precog
package daze

import bytecode.Library
import bytecode.BuiltInFunc1
import bytecode.BuiltInFunc2

import yggdrasil._

trait GenOpcode extends ImplLibrary {
  private val defaultUnaryOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
  abstract class BIF1(val namespace: Vector[String], val name: String, val opcode: Int = defaultUnaryOpcode.getAndIncrement) extends BuiltInFunc1Impl 

  private val defaultBinaryOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
  abstract class BIF2(val namespace: Vector[String], val name: String, val opcode: Int = defaultBinaryOpcode.getAndIncrement) extends BuiltInFunc2Impl
}

trait ImplLibrary extends Library {
  type Dataset[E]

  lazy val lib1 = _lib1
  lazy val lib2 = _lib2

  def _lib1: Set[BIF1] = Set()
  def _lib2: Set[BIF2] = Set()

  trait BuiltInFunc1Impl extends BuiltInFunc1 {
    val operation: PartialFunction[SValue, SValue]
    val operandType: Option[SType]
  }

  trait BuiltInFunc2Impl extends BuiltInFunc2 {
    val operation: PartialFunction[(SValue, SValue), SValue]
    val operandType: (Option[SType], Option[SType])
    
    val requiresReduction: Boolean = false
    def reduced(enum: Dataset[SValue]): Option[SValue] = None
  }

  type BIF1 <: BuiltInFunc1Impl
  type BIF2 <: BuiltInFunc2Impl
}

trait Stdlib extends Timelib with Infixlib with Mathlib with Stringlib with Statslib


