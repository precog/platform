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
package com.precog.bytecode

import org.scalacheck._
import Arbitrary.arbitrary
import Gen._

trait RandomLibrary extends Library {
  case class BIR(namespace: Vector[String], name: String, opcode: Int) extends BuiltInRed
  case class BIF1(namespace: Vector[String], name: String, opcode: Int) extends BuiltInFunc1
  case class BIF2(namespace: Vector[String], name: String, opcode: Int) extends BuiltInFunc2

  private lazy val genRed = for {
    op <- choose(0, 1000)
    n  <- identifier
    ns <- listOfN(2, identifier)
  } yield BIR(Vector(ns: _*), n, op)

  private lazy val genBuiltIn1 = for {
    op <- choose(0, 1000)
    n  <- identifier
    ns <- listOfN(2, identifier)
  } yield BIF1(Vector(ns: _*), n, op)

  private lazy val genBuiltIn2 = for {
    op <- choose(0, 1000)
    n  <- identifier
    ns <- listOfN(2, identifier)
  } yield BIF2(Vector(ns: _*), n, op)

  val reductions = Set(
    BIR(Vector(), "count", 0x2000),
    BIR(Vector(), "max", 0x2001),
    BIR(Vector(), "min", 0x2004), 
    BIR(Vector(), "sum", 0x2002),
    BIR(Vector(), "mean", 0x2013),
    BIR(Vector(), "geometricMean", 0x2003),
    BIR(Vector(), "sumSq", 0x2005),
    BIR(Vector(), "variance", 0x2006),
    BIR(Vector(), "stdDev", 0x2007),
    BIR(Vector(), "median", 0x2008),
    BIR(Vector(), "mode", 0x2009))
    
  lazy val libReduct = reductions ++ containerOfN[Set, BIR](30, genRed).sample.get.map(op => (op.opcode, op)).toMap.values.toSet //make sure no duplicate opcodes
  lazy val lib1 = containerOfN[Set, BIF1](30, genBuiltIn1).sample.get.map(op => (op.opcode, op)).toMap.values.toSet //make sure no duplicate opcodes
  lazy val lib2 = containerOfN[Set, BIF2](30, genBuiltIn2).sample.get.map(op => (op.opcode, op)).toMap.values.toSet //make sure no duplicate opcodes
}
