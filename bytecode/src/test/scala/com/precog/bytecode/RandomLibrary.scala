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
