/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
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
    
    abstract class Reduction(val namespace: Vector[String], val name: String) extends ReductionLike with Morphism1 with ((JValue, JValue) => JValue) {
      val opcode = 0x0001       // we really don't care
      val tpe = UnaryOperationType(JUniverseT, JUniverseT)
      val zero: Option[JValue]
      def prepare: PartialFunction[JValue, JValue]
      def apply(left: JValue, right: JValue): JValue
    }

    def libMorphism1 = Set()
    def libMorphism2 = Set()
    def lib1 = Set(math.sin)
    def lib2 = Set(math.roundTo)
    def libReduction = Set(reductions.count)
    
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
    
    object reductions {
      val Namespace = Vector()
      
      object count extends Reduction(Namespace, "count") {
        val zero = Some(JNum(0))
        
        override def prepare = {
          case v => JNum(1)
        }
        
        override def apply(left: JValue, right: JValue) = (left, right) match {
          case (JNum(l), JNum(r)) => JNum(l + r)
          case _ => JNum(0)
        }
      }
    }
  }
  
  object library extends Lib
}
