package com.precog.bytecode

trait StaticLibrary extends Library {

  lazy val libReduction = Set(
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
    Reduction(Vector(), "mode", 0x2009),
    Reduction(Vector(), "exists", 0x2010),
    Reduction(Vector(), "forall", 0x2011),
    Reduction(Vector("std", "lib"), "sum", 0x0010),
    Reduction(Vector("ack"), "ook", 0x0011),
    Reduction(Vector("one", "two", "three"), "qnd", 0x0012))
  
  lazy val lib1 = Set(
    Op1(Vector(), "bin", 0x0000),
    Op1(Vector("std"), "bin", 0x0001),
    Op1(Vector("std"), "lib", 0x0004),     // weird shadowing ahoy!
    Op1(Vector(), "bar", 0x0002),
    Op1(Vector("std", "lib"), "baz", 0x0003),
    Op1(Vector("std", "math"), "floor", 0x0005),
    Op1(Vector("std", "time"), "minuteOfHour", 0x0010),
    Op1(Vector("std", "time"), "hourOfDay", 0x0012),
    Op1(Vector("std", "time"), "getMillis", 0x0014))
  
  lazy val lib2 = Set(
    Op2(Vector(), "bin2", 0x0000),
    Op2(Vector("std"), "bin2", 0x0001),
    Op2(Vector(), "bar2", 0x0002),
    Op2(Vector("std", "lib"), "baz2", 0x0003),
    Op2(Vector("std", "time"), "millisToISO", 0x0013))
  
  lazy val libMorphism1 = Set(
    M1,
    DeprecatedM1,
    M11,
    Morphism1(Vector(), "bar33", 0x0002),
    Morphism1(Vector(), "denseRank", 0x0003),
    flatten)
    
  lazy val flatten = new Morphism1(Vector(), "flatten", 0x0100) {
    override val idPolicy = IdentityPolicy.Synthesize
  }
    
  lazy val libMorphism2 = Set(
    M2,
    Morphism2(Vector("std", "lib9"), "baz2", 0x0003))
    
  lazy val expandGlob = Morphism1(Vector("std", "fs"), "expandGlob", 0x0004)
  
  object DeprecatedM1 extends Morphism1(Vector(), "bin25", 0x0025) {
    override val idPolicy = IdentityPolicy.Retain.Merge
    override val deprecation = Some("use bin5 instead")
  }
  
  object M1 extends Morphism1(Vector(), "bin5", 0x0000) {
    override val idPolicy = IdentityPolicy.Retain.Merge
  }

  object M11 extends Morphism1(Vector("std", "random"), "foobar", 0x0006) {
    override val isInfinite = true
  }

  object M2 extends Morphism2(Vector("std"), "bin9", 0x0001) {
    override val idPolicy = IdentityPolicy.Retain.Merge
  }

  case class Morphism1(namespace: Vector[String], name: String, opcode: Int) extends Morphism1Like {
    val tpe = UnaryOperationType(JType.JUniverseT, JType.JUniverseT)
  }
  
  case class Morphism2(namespace: Vector[String], name: String, opcode: Int) extends Morphism2Like {
    val tpe = BinaryOperationType(JType.JUniverseT, JType.JUniverseT, JType.JUniverseT)
  }
  
  case class Op1(namespace: Vector[String], name: String, opcode: Int) extends Op1Like with Morphism1Like {
    val tpe = UnaryOperationType(JType.JUniverseT, JType.JUniverseT)
  }
  
  case class Op2(namespace: Vector[String], name: String, opcode: Int) extends Op2Like with Morphism2Like {
    val tpe = BinaryOperationType(JType.JUniverseT, JType.JUniverseT, JType.JUniverseT)
  }
  
  case class Reduction(namespace: Vector[String], name: String, opcode: Int) extends ReductionLike with Morphism1Like {
    val tpe = UnaryOperationType(JType.JUniverseT, JType.JUniverseT)
  }
}
