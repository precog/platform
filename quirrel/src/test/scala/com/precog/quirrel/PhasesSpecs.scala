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
package quirrel

import org.specs2.mutable._

import parser._
import typer._
import emitter._

object PhasesSpecs extends Specification
    with StubPhases
    with CompilerUtils
    with Compiler
    with ProvenanceChecker
    with GroupSolver
    with RawErrors
    with RandomLibrarySpec {

  "full compiler" should {
    "self-populate AST errors atom" in {
      val tree = compileSingle("fubar")
      tree.errors must not(beEmpty)
    }
    
    "propagate binder errors through tree shaking" in {
      val input = """
        | f( x ) :=
        |   new x ~ new x
        |   (new x) + (new x)
        | f(5)
        | """.stripMargin
        
      val forest = compile(input)
      val validForest = forest filter { tree =>
        tree.errors forall isWarning
      }
      
      validForest must beEmpty
    }
  }
}
