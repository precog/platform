package com.precog
package mirror

import bytecode._
import quirrel._
import quirrel.typer.Binder

trait LibraryModule extends Binder {
  
  // TODO do something interesting here
  class Lib extends Library {
    type Morphism1 = Morphism1Like
    type Morphism2 = Morphism2Like
    type Op1 = Op1Like with Morphism1
    type Op2 = Op2Like
    type Reduction = ReductionLike with Morphism1

    def libMorphism1 = Set()
    def libMorphism2 = Set()
    def lib1 = Set()
    def lib2 = Set()
    def libReduction = Set()
    
    lazy val expandGlob = new Morphism1Like {
      val namespace = Vector("std", "fs")
      val name = "expandGlob"
      val opcode = 0x0001
      val tpe = UnaryOperationType(JType.JUniverseT, JType.JUniverseT)
    }
  }
  
  object library extends Lib
}
