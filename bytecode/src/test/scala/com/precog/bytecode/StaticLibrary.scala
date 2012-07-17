package com.precog.bytecode

trait StaticLibrary extends Library {

  lazy val libReduction = Set(
    Reduction(Vector(), "count", 0x0000),
    Reduction(Vector(), "max", 0x0001),
    Reduction(Vector(), "min", 0x0004),
    Reduction(Vector(), "sum", 0x0002),
    Reduction(Vector(), "mean", 0x0013),
    Reduction(Vector(), "geometricMean", 0x0003),
    Reduction(Vector(), "sumSq", 0x0005),
    Reduction(Vector(), "variance", 0x0006),
    Reduction(Vector(), "stdDev", 0x0007),
    Reduction(Vector(), "median", 0x0008),
    Reduction(Vector(), "mode", 0x0009),
    Reduction(Vector("std", "lib"), "sum", 0x0010),
    Reduction(Vector("ack"), "ook", 0x0011),
    Reduction(Vector("one", "two", "three"), "qnd", 0x0012))
  
  lazy val lib1 = Set(
    Op1(Vector(), "bin", 0x0000),
    Op1(Vector("std"), "bin", 0x0001),
    Op1(Vector("std"), "lib", 0x0004),     // weird shadowing ahoy!
    Op1(Vector(), "bar", 0x0002),
    Op1(Vector("std", "lib"), "baz", 0x0003))
  
  lazy val lib2 = Set(
    Op2(Vector(), "bin2", 0x0000),
    Op2(Vector("std"), "bin2", 0x0001),
    Op2(Vector(), "bar2", 0x0002),
    Op2(Vector("std", "lib"), "baz2", 0x0003))
  
  lazy val libMorphism = Set(
    Morphism(Vector(), "bin5", 0x0000, Arity.One),
    Morphism(Vector("std"), "bin9", 0x0001, Arity.Two),
    Morphism(Vector(), "bar33", 0x0002, Arity.One),
    Morphism(Vector("std", "lib9"), "baz2", 0x0003, Arity.Two))
  
  
  case class Morphism(namespace: Vector[String], name: String, opcode: Int, arity: Arity) extends MorphismLike
  
  case class Op1(namespace: Vector[String], name: String, opcode: Int) extends Op1Like with MorphismLike {
    lazy val arity = Arity.One // MS: Why lazy?
    val tpe = UnaryOperationType(JType.JUnfixedT, JType.JUnfixedT)
  }
  
  case class Op2(namespace: Vector[String], name: String, opcode: Int) extends Op2Like with MorphismLike {
    lazy val arity = Arity.Two // MS: Why lazy?
    val tpe = BinaryOperationType(JType.JUnfixedT, JType.JUnfixedT, JType.JUnfixedT)
  }
  
  case class Reduction(namespace: Vector[String], name: String, opcode: Int) extends ReductionLike with MorphismLike {
    lazy val arity = Arity.One // MS: Why lazy?
  }
}
