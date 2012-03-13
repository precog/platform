package com.precog

package daze

import yggdrasil._
import bytecode.BuiltInFunc1
import bytecode.BuiltInFunc2
import bytecode.Library

trait Genlib extends Library {
  lazy val lib = _lib
  def _lib: Set[BIF1] = Set()
}

trait GenLibrary extends GenOpcode with Genlib {
  override def _lib = super._lib ++ Set(Foo)

  object Foo extends BIF1(Vector(), "foo") {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = {
      case SDecimal(num) => SDecimal(num + 1)
    }
  }
}
