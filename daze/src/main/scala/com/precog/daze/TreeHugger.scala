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



  
