package com.precog.quirrel

import org.specs2.mutable._

import com.codecommit.gll._

trait CompilerUtils extends Specification with Compiler {
  def compileSingle(str: LineStream): Expr = {
    val forest = compile(str)
    forest must haveSize(1)
    forest.head
  }
  
  def compileSingle(str: String): Expr = compileSingle(LineStream(str))
}
