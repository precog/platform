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

import treehugger.forest._
import definitions._
import treehuggerDSL._

import java.io._

trait UsefulStuff {
  val str: String
}

object Code {

}

class Code extends UsefulStuff { 
  object sym {
    val _lib = RootClass.newValue("_lib")
    val Set: ClassSymbol = RootClass.newClass("Set")
    val PartialFunction: ClassSymbol = RootClass.newClass("PartialFunction[SValue, SValue]")
    val SDecimal: ClassSymbol = RootClass.newClass("SDecimal")
    val num = RootClass.newValue("num")
  }

  val import1 = IMPORT("bytecode.Library")
  val import3 = IMPORT("bytecode.BuiltInFunc1")
  val import4 = IMPORT("bytecode.BuiltInFunc2")
  val import2 = IMPORT("yggdrasil._") inPackage("daze") inPackage("com.precog") 
  
  
  val trait1 = {
    TRAITDEF("GenLibrary") withParents("Library", "GenOpcode") := BLOCK(
      LAZYVAL("lib") := REF("_lib"),
      DEF("_lib") := REF("Set(Foo)"),
    
      OBJECTDEF("Foo") withParents("""BIF1(Vector(), "foo")""") := BLOCK(
        VAL("operandType") := (REF("Some(SDecimal)")),
        VAL("operation", sym.PartialFunction) := BLOCK(
          CASE(REF("SDecimal(num)")) ==> REF("""SDecimal(num + 1)"""))))
  }

  val trees = import2 :: import3 :: import4 :: import1 :: trait1 :: Nil

  val str: String = treeToString(trees: _*) //eventually will concatate all trees into a single Tree, which can then be printed as a String

  val fileInstance = new File("daze/src/main/scala/com/precog/daze/Genlib.scala")
  fileInstance.delete()
  fileInstance.createNewFile()
  val writer = new FileWriter(fileInstance)

  writer.write(str)
  writer.close()
}



  
