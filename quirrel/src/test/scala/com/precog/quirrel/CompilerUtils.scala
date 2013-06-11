package com.precog.quirrel

import org.specs2.mutable._

import com.codecommit.gll._

trait CompilerUtils extends Specification with Compiler with Errors {
  def compileSingle(str: LineStream): Expr = {
    val forest = compile(str)
    val validForest = forest filter { tree =>
      tree.errors forall isWarning
    }
    
    if (validForest.size == 1) {
      validForest.head
    } else {
      forest must haveSize(1)
      forest.head
    }
  }
  
  def compileSingle(str: String): Expr = compileSingle(LineStream(str))
}
