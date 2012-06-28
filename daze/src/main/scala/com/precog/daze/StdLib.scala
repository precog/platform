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

import memoization._

import bytecode.Library
import bytecode.BuiltInRed
import bytecode.BuiltInFunc1
import bytecode.BuiltInFunc2

import yggdrasil._

trait GenOpcode extends ImplLibrary {
  private val defaultMorphismOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
  abstract class Morphism(val namespace: Vector[String], val name: String, val opcode: Int = defaultMorphismOpcode.getAndIncrement) extends MorphismImpl 

  private val defaultUnaryOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
  abstract class Op1(val namespace: Vector[String], val name: String, val opcode: Int = defaultUnaryOpcode.getAndIncrement) extends Op1Impl

  private val defaultBinaryOpcode = new java.util.concurrent.atomic.AtomicInteger(0)
  abstract class Op2(val namespace: Vector[String], val name: String, val opcode: Int = defaultBinaryOpcode.getAndIncrement) extends Op2Impl
}

trait ImplLibrary extends Library {
  type Table
  type F1
  type F2
  
  lazy val libMorphism = _libMorphism
  lazy val lib1 = _lib1
  lazy val lib2 = _lib2

  def _libMorphism: Set[Morphism] = Set()
  def _lib1: Set[Op1] = Set()
  def _lib2: Set[Op2] = Set()

  trait MorphismImpl extends MorphismLike {
    def alignment: Option[MorphismAlignment]
    def apply(input: Table): Table
  }
  
  sealed trait MorphismAlignment
  
  object MorphismAlignment {
    case object Match extends MorphismAlignment
    case object Cross extends MorphismAlignment
  }

  trait Op1Impl extends Op1Like with MorphismImpl {
    lazy val alignment = None
    def f1: F1
  }

  trait BuiltInFunc2Impl extends BuiltInFunc2 {
    lazy val alignment = None
    def f2: F2
  }

  type Morphism <: MorphismImpl
  type Op1 <: Op1Impl
  type Op2 <: Op2Impl
}

trait StdLib extends InfixLib with ReductionLib with TimeLib with MathLib with StringLib with StatsLib 


