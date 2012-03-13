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
  val packages = 
    (IMPORT("bytecode.Library")
    ) inPackage("daze") inPackage("com.precog") 
  
  val trait1 = {
    TRAITDEF("GeneratedLib") withParents("ImplLibrary"):= BLOCK(
      VAL("operandType") := SOME(ID("SDecimal")))
  }

  val trees = packages :: trait1 :: Nil

  val str: String = treeToString(trees: _*) //eventually will concatate all trees into a single Tree, which can then be printed as a String
  
  
  val file = File.createNewFile()
  val writer = new FileWriter(file)

  writer.write(str)
  writer.close()
}



  
