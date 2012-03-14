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
  
  val imports = BLOCK(IMPORT("bytecode.Library") :: IMPORT("bytecode.BuiltInFunc1") :: IMPORT("java.lang.Math") :: IMPORT("java.lang.String") :: IMPORT("bytecode.BuiltInFunc2") :: IMPORT("yggdrasil._") :: Nil: _*) inPackage("daze") inPackage("com.precog")

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
      LAZYVAL("mathlib1") := REF("_mathlib1"),
      LAZYVAL("mathlib2") := REF("_mathlib2"),
      DEF("_mathlib1", sym.BIF1) := REF("Set()"),
      DEF("_mathlib2", sym.BIF2) := REF("Set()")
    ) 
  }

  def trait2: Tree = {
    TRAITDEF("Genlib") withParents("GenOpcode", "GenLibrary") := BLOCK(
      (DEF("_mathlib1") withFlags(Flags.OVERRIDE) := REF("super._mathlib1") SEQ_++ (sym.Set UNAPPLY(ID(m1)))) ::
      (DEF("_mathlib2") withFlags(Flags.OVERRIDE) := REF("super._mathlib2") SEQ_++ (sym.Set UNAPPLY(ID(m2)))) :: 
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

  val trees = imports :: trait1 :: trait2 :: Nil 

  val str: String = treeToString(trees: _*) 

  val fileInstance = new File("daze/src/main/scala/com/precog/daze/Genlib.scala")
  fileInstance.delete()
  fileInstance.createNewFile()
  val writer = new FileWriter(fileInstance)

  writer.write(str)
  writer.close()
}



  
