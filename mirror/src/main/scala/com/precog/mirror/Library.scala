package com.precog
package mirror

import bytecode._
import quirrel._
import quirrel.typer.Binder
import util._

import blueeyes.json._

trait LibraryModule extends Binder {
  import JType.JUniverseT
  
  // TODO do something interesting here
  class Lib extends Library {
    type Morphism1 = Morphism1Like
    type Morphism2 = Morphism2Like
    
    abstract class Op1(val namespace: Vector[String], val name: String) extends Op1Like with Morphism1 {
      val opcode = 0x0001       // we really don't care
      val tpe = UnaryOperationType(JUniverseT, JUniverseT)
      def pf: PartialFunction[JValue, JValue]
    }
    
    abstract class Op2(val namespace: Vector[String], val name: String) extends Op2Like {
      val opcode = 0x0001       // we really don't care
      val tpe = BinaryOperationType(JUniverseT, JUniverseT, JUniverseT)
      def pf: PartialFunction[(JValue, JValue), JValue]
    }
    
    type Reduction = ReductionLike with Morphism1

    def libMorphism1 = Set()
    def libMorphism2 = Set()
    def lib1 = Set(math.sin)
    def lib2 = Set(math.roundTo)
    def libReduction = Set()
    
    lazy val expandGlob = new Morphism1Like {
      val namespace = Vector("std", "fs")
      val name = "expandGlob"
      val opcode = 0x0001
      val tpe = UnaryOperationType(JType.JUniverseT, JType.JUniverseT)
    }
    
    object math {
      val Namespace = Vector("std", "math")
      
      object sin extends Op1(Namespace, "sin") {
        override def pf = {
          case JNum(num) => JNum(scala.math.sin(num.toDouble))
        }
      }
      
      object roundTo extends Op2(Namespace, "roundTo") {
        override def pf = {
          case (JNum(nBD), JNum(digitsBD)) => {
            val n = nBD.toDouble
            val digits = digitsBD.toDouble
            
            val adjusted = n * scala.math.pow(10, digits)
            val rounded = if (scala.math.abs(n) >= 4503599627370496.0) adjusted else scala.math.round(adjusted)
            JNum(rounded * scala.math.pow(10, -digits))
          }
        }
      }
    }
  }
  
  object library extends Lib
}
