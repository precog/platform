package com.precog.bytecode

trait StaticLibrary extends Library {
  case class ReductionBinding(namespace: Vector[String], name: String, opcode: Int) extends ReductionLike
  case class Op1Binding(namespace: Vector[String], name: String, opcode: Int) extends Op1Like
  case class Op2Binding(namespace: Vector[String], name: String, opcode: Int) extends Op2Like
  case class MorphismBinding(namespace: Vector[String], name: String, opcode: Int) extends MorphismLike

  lazy val libReduction = Set(
    ReductionBinding(Vector(), "count", 0x0000),
    ReductionBinding(Vector(), "max", 0x0001),
    ReductionBinding(Vector(), "min", 0x0004),
    ReductionBinding(Vector(), "sum", 0x0002),
    ReductionBinding(Vector(), "mean", 0x0013),
    ReductionBinding(Vector(), "geometricMean", 0x0003),
    ReductionBinding(Vector(), "sumSq", 0x0005),
    ReductionBinding(Vector(), "variance", 0x0006),
    ReductionBinding(Vector(), "stdDev", 0x0007),
    ReductionBinding(Vector(), "median", 0x0008),
    ReductionBinding(Vector(), "mode", 0x0009),
    ReductionBinding(Vector("std", "lib"), "sum", 0x0010),
    ReductionBinding(Vector("ack"), "ook", 0x0011),
    ReductionBinding(Vector("one", "two", "three"), "qnd", 0x0012))
  
  lazy val lib1 = Set(
    Op1Binding(Vector(), "bin", 0x0000),
    Op1Binding(Vector("std"), "bin", 0x0001),
    Op1Binding(Vector("std"), "lib", 0x0004),     // weird shadowing ahoy!
    Op1Binding(Vector(), "bar", 0x0002),
    Op1Binding(Vector("std", "lib"), "baz", 0x0003))
  
  lazy val lib2 = Set(
    Op2Binding(Vector(), "bin2", 0x0000),
    Op2Binding(Vector("std"), "bin2", 0x0001),
    Op2Binding(Vector(), "bar2", 0x0002),
    Op2Binding(Vector("std", "lib"), "baz2", 0x0003))
  
  lazy val libMorphism = Set(
    MorphismBinding(Vector(), "bin5", 0x0000),
    MorphismBinding(Vector("std"), "bin9", 0x0001),
    MorphismBinding(Vector(), "bar33", 0x0002),
    MorphismBinding(Vector("std", "lib9"), "baz2", 0x0003))
}
