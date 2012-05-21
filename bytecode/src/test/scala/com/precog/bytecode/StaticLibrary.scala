package com.precog.bytecode

trait StaticLibrary extends Library {
  case class BIF1(namespace: Vector[String], name: String, opcode: Int) extends BuiltInFunc1
  case class BIF2(namespace: Vector[String], name: String, opcode: Int) extends BuiltInFunc2

  lazy val lib1 = Set(
    BIF1(Vector(), "bin", 0x0000),
    BIF1(Vector("std"), "bin", 0x0001),
    BIF1(Vector("std"), "lib", 0x0004),     // weird shadowing ahoy!
    BIF1(Vector(), "bar", 0x0002),
    BIF1(Vector("std", "lib"), "baz", 0x0003))
  
  lazy val lib2 = Set(
    BIF2(Vector(), "bin2", 0x0000),
    BIF2(Vector("std"), "bin2", 0x0001),
    BIF2(Vector(), "bar2", 0x0002),
    BIF2(Vector("std", "lib"), "baz2", 0x0003))
}
