package com.precog
package quirrel

import com.codecommit.gll.LineStream

import util.BitSet

import com.precog.quirrel._
import bytecode._
import emitter._
import parser._
import typer._

object QuirrelConsole {
  val compiler = new Parser
      with StubPhases
      with ProvenanceChecker
      with Binder
      with Compiler
      with Emitter
      with LineErrors {

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
        val rowLevel = false
      }
    }

    object library extends Lib
  }

  trait StubPhases extends Phases {
    protected def LoadId = Identifier(Vector(), "load")
    protected def RelLoadId = Identifier(Vector(), "relativeLoad")
    protected def ExpandGlobId = Identifier(Vector("std", "fs"), "expandGlob")
    protected def DistinctId = Identifier(Vector(), "distinct")
    
    def bindNames(tree: Expr) = Set()
    def checkProvenance(tree: Expr) = Set()
    def findCriticalConditions(expr: Expr): Map[String, Set[ConditionTree]] = Map()
    def inferBuckets(expr: Expr) = Set()
    def buildTrace(sigma: Sigma)(expr: Expr): Trace = Trace.empty
  }
}
