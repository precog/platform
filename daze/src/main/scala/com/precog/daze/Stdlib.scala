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


