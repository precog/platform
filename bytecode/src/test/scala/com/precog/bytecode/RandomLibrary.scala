package com.precog.bytecode

import org.scalacheck._
import Arbitrary.arbitrary
import Gen._

trait RandomLibrary extends Library {
  case class BIF1(namespace: Vector[String], name: String, opcode: Int) extends BuiltInFunc1
  case class BIF2(namespace: Vector[String], name: String, opcode: Int) extends BuiltInFunc2

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
    
  lazy val lib1 = containerOfN[Set, BIF1](30, genBuiltIn1).sample.get.map(op => (op.opcode, op)).toMap.values.toSet //make sure no duplicate opcodes
  lazy val lib2 = containerOfN[Set, BIF2](30, genBuiltIn2).sample.get.map(op => (op.opcode, op)).toMap.values.toSet //make sure no duplicate opcodes
}
