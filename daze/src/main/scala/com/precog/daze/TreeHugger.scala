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
    val BIF1: ClassSymbol = RootClass.newClass("Set[BIF1]")
    val sup = RootClass.newValue("super._lib")
  }

  val import1 = IMPORT("bytecode.Library")
  val import3 = IMPORT("bytecode.BuiltInFunc1")
  val import5 = IMPORT("java.lang.Math")
  val import4 = IMPORT("bytecode.BuiltInFunc2")
  val import2 = IMPORT("yggdrasil._") inPackage("daze") inPackage("com.precog") 


  val methods: Array[String] = classOf[Math].getMethods.map(_.getName)
  val parameters = classOf[Math].getMethods.map(_.getParameterTypes)
  val map = (methods zip parameters) toMap 
  val filteredMap = map.filter { case ((_, par)) => par.length == 1 }
  val methods2 = filteredMap.keySet.toList diff List("equals") 

  val mtdString: String = methods2.foldLeft("")((acc, e) => acc + e + ", ").dropRight(2)
 
  val trait1: Tree = {
    TRAITDEF("Genlib") withParents("Library") := BLOCK(
      LAZYVAL("lib") := REF("_lib"),
      DEF("_lib", sym.BIF1) := REF("Set()"))
  }

  def trait2: Tree = {
    TRAITDEF("GenLibrary") withParents("GenOpcode", "Genlib") := BLOCK(
      DEF("_lib") withFlags(Flags.OVERRIDE) := REF("super._lib") SEQ_++ (sym.Set UNAPPLY(ID(mtdString))))
  }

  def objects(method: String): Tree = {
    OBJECTDEF(method) withParents("""BIF1(Vector(), "%s")""".format(method)) := BLOCK(
      VAL("operandType") := (REF("Some(SDecimal)")),
      VAL("operation", sym.PartialFunction) := BLOCK(
        CASE(REF("SDecimal(num)")) ==> REF("""SDecimal(Math.%s(num.toDouble))""".format(method))))
      
    
  }

  val methodsFinal = methods2 map { x => objects(x) }

  val trees = import2 :: import3 :: import4 :: import5 :: import1 :: trait1 :: trait2 :: methodsFinal

  val str: String = treeToString(trees: _*) //eventually will concatate all trees into a single Tree, which can then be printed as a String

  val fileInstance = new File("daze/src/main/scala/com/precog/daze/Genlib.scala")
  fileInstance.delete()
  fileInstance.createNewFile()
  val writer = new FileWriter(fileInstance)

  writer.write(str)
  writer.close()
}



  
