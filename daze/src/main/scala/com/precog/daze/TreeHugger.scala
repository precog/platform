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
    val Set: ClassSymbol = RootClass.newClass("Set")
    val PartialFunction1: ClassSymbol = RootClass.newClass("PartialFunction[SValue, SValue]")
    val PartialFunction2: ClassSymbol = RootClass.newClass("PartialFunction[(SValue, SValue), SValue]")
    val BIF1: ClassSymbol = RootClass.newClass("Set[BIF1]")
    val BIF2: ClassSymbol = RootClass.newClass("Set[BIF2]")
  }

  val import1 = IMPORT("bytecode.Library")
  val import3 = IMPORT("bytecode.BuiltInFunc1")
  val import5 = IMPORT("java.lang.Math")
  val import4 = IMPORT("bytecode.BuiltInFunc2")
  val import2 = IMPORT("yggdrasil._") inPackage("daze") inPackage("com.precog") 


  val methods: Array[String] = classOf[Math].getMethods.map(_.getName)
  val parameters = classOf[Math].getMethods.map(_.getParameterTypes)
  val map = (methods zip parameters) toMap 
  val arityOne = map.filter { case ((_, par)) => par.length == 1 }
  val arityTwo = map.filter { case ((_, par)) => par.length == 2 }
  val methodsOne = arityOne.keySet.toList diff List("equals") 
  val methodsTwo = arityTwo.keySet.toList diff List("scalb", "wait")

  val m1: String = methodsOne.foldLeft("")((acc, e) => acc + e + ", ").dropRight(2)
  val m2: String = methodsTwo.foldLeft("")((acc, e) => acc + e + ", ").dropRight(2)
 
  val trait1: Tree = {
    TRAITDEF("GenLibrary") withParents("Library") := BLOCK(
      LAZYVAL("genlib1") := REF("_genlib1"),
      LAZYVAL("genlib2") := REF("_genlib2"),
      DEF("_genlib1", sym.BIF1) := REF("Set()"),
      DEF("_genlib2", sym.BIF2) := REF("Set()")
    ) 
  }

  def trait2: Tree = {
    TRAITDEF("Genlib") withParents("GenOpcode", "GenLibrary") := BLOCK(
      (DEF("_genlib1") withFlags(Flags.OVERRIDE) := REF("super._genlib1") SEQ_++ (sym.Set UNAPPLY(ID(m1)))) ::
      (DEF("_genlib2") withFlags(Flags.OVERRIDE) := REF("super._genlib2") SEQ_++ (sym.Set UNAPPLY(ID(m2)))) :: 
      methodsAll: _*
    )
  }

  def objects1(method: String): Tree = {
    OBJECTDEF(method) withParents("""BIF1(Vector("std", "math"), "%s")""".format(method)) := BLOCK(
      VAL("operandType") := (REF("Some(SDecimal)")),
      VAL("operation", sym.PartialFunction1) := BLOCK(
        CASE(REF("SDecimal(num)")) ==> REF("""SDecimal(Math.%s(num.toDouble))""".format(method))))
  }
      
  def objects2(method: String): Tree = {
    OBJECTDEF(method) withParents("""BIF2(Vector("std", "math"), "%s")""".format(method)) := BLOCK(
      VAL("operandType") := (REF("(Some(SDecimal), Some(SDecimal))")),
      VAL("operation", sym.PartialFunction2) := BLOCK(
        CASE(REF("(SDecimal(num1), SDecimal(num2))")) ==> REF("""SDecimal(Math.%s(num1.toDouble, num2.toDouble))""".format(method))))
  }

  val methodsOneGen = methodsOne map { x => objects1(x) }
  val methodsTwoGen = methodsTwo map { x => objects2(x) }
  val methodsAll = methodsOneGen ++ methodsTwoGen

  val trees = import2 :: import3 :: import4 :: import5 :: import1 :: trait1 :: trait2 :: Nil 

  val str: String = treeToString(trees: _*) 

  val fileInstance = new File("daze/src/main/scala/com/precog/daze/Genlib.scala")
  fileInstance.delete()
  fileInstance.createNewFile()
  val writer = new FileWriter(fileInstance)

  writer.write(str)
  writer.close()
}



  
