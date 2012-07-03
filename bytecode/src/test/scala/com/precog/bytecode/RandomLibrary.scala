package com.precog.bytecode

import org.scalacheck._
import Arbitrary.arbitrary
import Gen._

trait RandomLibrary extends Library {
  case class Morphism(namespace: Vector[String], name: String, opcode: Int, arity: Arity) extends MorphismLike
  case class Op1(namespace: Vector[String], name: String, opcode: Int) extends Op1Like
  case class Op2(namespace: Vector[String], name: String, opcode: Int) extends Op2Like
  case class Reduction(namespace: Vector[String], name: String, opcode: Int) extends ReductionLike

  private lazy val genMorphism = for {
    op <- choose(0, 1000)
    a  <- choose(1, 2) map { case 1 => Arity.One; case 2 => Arity.Two }
    n  <- identifier
    ns <- listOfN(2, identifier)
  } yield Morphism(Vector(ns: _*), n, op, a)

  private lazy val genOp1 = for {
    op <- choose(0, 1000)
    n  <- identifier
    ns <- listOfN(2, identifier)
  } yield Op1(Vector(ns: _*), n, op)

  private lazy val genOp2 = for {
    op <- choose(0, 1000)
    n  <- identifier
    ns <- listOfN(2, identifier)
  } yield Op2(Vector(ns: _*), n, op)

  private lazy val genReduction = for {
    op <- choose(0, 1000)
    n  <- identifier
    ns <- listOfN(2, identifier)
  } yield Reduction(Vector(ns: _*), n, op)

  val reductions = Set(
    Reduction(Vector(), "count", 0x2000),
    Reduction(Vector(), "max", 0x2001),
    Reduction(Vector(), "min", 0x2004), 
    Reduction(Vector(), "sum", 0x2002),
    Reduction(Vector(), "mean", 0x2013),
    Reduction(Vector(), "geometricMean", 0x2003),
    Reduction(Vector(), "sumSq", 0x2005),
    Reduction(Vector(), "variance", 0x2006),
    Reduction(Vector(), "stdDev", 0x2007),
    Reduction(Vector(), "median", 0x2008),
    Reduction(Vector(), "mode", 0x2009))

    
  lazy val libMorphism = containerOfN[Set, Morphism](30, genMorphism).sample.get.map(op => (op.opcode, op)).toMap.values.toSet //make sure no duplicate opcodes
  lazy val lib1 = containerOfN[Set, Op1](30, genOp1).sample.get.map(op => (op.opcode, op)).toMap.values.toSet //make sure no duplicate opcodes
  lazy val lib2 = containerOfN[Set, Op2](30, genOp2).sample.get.map(op => (op.opcode, op)).toMap.values.toSet //make sure no duplicate opcodes
  lazy val libReduction = reductions ++ containerOfN[Set, Reduction](30, genReduction).sample.get.map(op => (op.opcode, op)).toMap.values.toSet //make sure no duplicate opcodes
}
