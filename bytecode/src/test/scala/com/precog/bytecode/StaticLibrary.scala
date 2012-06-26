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

trait StaticLibrary extends Library {
  case class BIR(namespace: Vector[String], name: String, opcode: Int) extends BuiltInRed
  case class BIF1(namespace: Vector[String], name: String, opcode: Int, isOperation: Boolean) extends BuiltInFunc1
  case class BIF2(namespace: Vector[String], name: String, opcode: Int, isOperation: Boolean) extends BuiltInFunc2

  lazy val libReduct = Set(
    BIR(Vector(), "count", 0x0000),
    BIR(Vector(), "max", 0x0001),
    BIR(Vector(), "min", 0x0004),
    BIR(Vector(), "sum", 0x0002),
    BIR(Vector(), "mean", 0x0013),
    BIR(Vector(), "geometricMean", 0x0003),
    BIR(Vector(), "sumSq", 0x0005),
    BIR(Vector(), "variance", 0x0006),
    BIR(Vector(), "stdDev", 0x0007),
    BIR(Vector(), "median", 0x0008),
    BIR(Vector(), "mode", 0x0009),
    BIR(Vector("std", "lib"), "sum", 0x0010),
    BIR(Vector("ack"), "ook", 0x0011),
    BIR(Vector("one", "two", "three"), "qnd", 0x0012))
  
  lazy val lib1 = Set(
    BIF1(Vector(), "bin", 0x0000, true),
    BIF1(Vector("std"), "bin", 0x0001, true),
    BIF1(Vector("std"), "lib", 0x0004, false),     // weird shadowing ahoy!
    BIF1(Vector(), "bar", 0x0002, false),
    BIF1(Vector("std", "lib"), "baz", 0x0003, true))
  
  lazy val lib2 = Set(
    BIF2(Vector(), "bin2", 0x0000, false),
    BIF2(Vector("std"), "bin2", 0x0001, true),
    BIF2(Vector(), "bar2", 0x0002, true),
    BIF2(Vector("std", "lib"), "baz2", 0x0003, false))
}
